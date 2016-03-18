#!/usr/bin/env python2.7

import argparse
import multiprocessing
import os
import subprocess

from toil.job import Job

from toil_scripts import download_from_s3_url
from toil_scripts.batch_alignment.bwa_alignment import upload_or_move, docker_call
from toil_scripts.gatk_germline.germline import download_url, read_from_filestore_hc

def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--reference', required=True, help='Reference Genome URL')
    parser.add_argument('-i', '--reference_index', required=True, help='Reference Genome index (.fai) URL')
    parser.add_argument('-d', '--reference_dict', required=True, help='Reference Genome sequence dictionary (.dict) URL')
    parser.add_argument('-1', '--eval_vcf', required=True, help='VCF file URL to evaluate')
    parser.add_argument('-y', '--eval_label', required=False, help='Optional label for VCF file to evaluate, defaults to file name')
    parser.add_argument('-2', '--comp_vcf', required=True, help='VCF file URL to compare against')
    parser.add_argument('-z', '--comp_label', required=False, help='Optional label for VCF file to compare against, defaults to file name')
    parser.add_argument('-o', '--output_dir', required=False, default=None, help='Local output directory, use one of this option or --s3_dir')
    parser.add_argument('-3', '--s3_dir', required=False, default=None, help='S3 output directory, starting with bucket name, use one of this option or --output_dir')
    return parser


def batch_start(job, input_args):
    shared_files = ['ref.fa', 'ref.fa.fai', 'ref.dict', 'eval.vcf', 'comp.vcf']
    shared_ids = {}
    for file_name in shared_files:
        url = input_args[file_name]
        shared_ids[file_name] = job.addChildJobFn(download_url, url, file_name).rv()
    job.addFollowOnJobFn(concordance, shared_ids, input_args)
    

def concordance(job, shared_ids, input_args):
    """
    Evaluate concordance between two VCFs with GATK.
    """
    work_dir = job.fileStore.getLocalTempDir()
    input_files = ['ref.fa', 'ref.fa.fai', 'ref.dict', 'eval.vcf', 'comp.vcf']
    read_from_filestore_hc(job, work_dir, shared_ids, *input_files)

    eval_name = input_args.get('eval_label', os.path.basename(input_args['eval.vcf']))
    comp_name = input_args.get('comp_label', os.path.basename(input_args['comp.vcf']))
    output = '%s_vs_%s.concordance' % (eval_name, comp_name)
    command = ['-T', 'GenotypeConcordance',
               '-R', 'ref.fa',
               '-nct', input_args['cpu_count'],
               '--eval', 'eval.vcf',
               '--comp', 'comp.vcf',
               '-o', output]

    docker_call(work_dir = work_dir,
                tool_parameters = command,
                tool = 'quay.io/ucsc_cgl/gatk',
                sudo = input_args['sudo'])

    upload_or_move(work_dir, input_args, output)


if __name__ == '__main__':
    args_parser = build_parser()
    Job.Runner.addToilOptions(args_parser)
    args = args_parser.parse_args()

    inputs = {'ref.fa': args.reference,
              'ref.fa.fai': args.reference_index,
              'ref.dict': args.reference_dict,
              'eval.vcf': args.eval_vcf,
              'eval_label': args.eval_label,
              'comp.vcf': args.comp_vcf,
              'comp_label': args.comp_label,
              'output_dir': args.output_dir,
              's3_dir': args.s3_dir,
              'uuid': None,
              # todo: this is req'd by gatk and by upload_to_s3
              'cpu_count': str(multiprocessing.cpu_count()),
              'ssec': None,
              'sudo': False}

    Job.Runner.startToil(Job.wrapJobFn(batch_start, inputs), args)
