#!/usr/bin/env python2.7

import argparse
import multiprocessing
import os
import subprocess

from toil.job import Job

from toil_scripts import download_from_s3_url
from toil_scripts.batch_alignment.bwa_alignment import upload_to_s3, docker_call
from toil_scripts.gatk_germline.germline import download_url, read_from_filestore_hc

def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--reference', required=True, help="Reference Genome URL")
    parser.add_argument('-i', '--reference_index', required=True, help="Reference Genome index (.fai) URL")
    parser.add_argument('-d', '--reference_dict', required=True, help="Reference Genome sequence dictionary (.dict) URL")
    parser.add_argument('-1', '--eval_vcf', required=True, help="VCF file URL to evaluate")
    parser.add_argument('-2', '--comp_vcf', required=True, help="VCF file URL to compare against")
    parser.add_argument('-o', '--output_dir', required=True, help="Output directory S3 URL")
    return parser


def batch_start(job, input_args):
    shared_files = ['ref.fa', 'ref.fa.fai', 'ref.fa.dict', 'eval.vcf', 'comp.vcf']
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
    input_files = ['ref.fa', 'ref.fa.fai', 'ref.fa.dict', 'eval.vcf', 'comp.vcf']
    read_from_filestore_hc(job, work_dir, shared_ids, *input_files)

    eval_name = input_args['eval.vcf'].split('/')[-1]
    comp_name = input_args['comp.vcf'].split('/')[-1]
    output = '%s_vs_%s.concordance' % (eval_name, comp_name)
    command = ['-T', 'GenotypeConcordance',
               '-R', 'ref.fa',
               '--eval', 'eval.vcf',
               '--comp', 'comp.vcf',
               '-o', output]

    docker_call(work_dir = work_dir,
                tool_parameters = command,
                tool = 'quay.io/ucsc_cgl/gatk',
                sudo = input_args['sudo'])

    upload_to_s3(work_dir, input_args, output)


if __name__ == '__main__':
    args_parser = build_parser()
    Job.Runner.addToilOptions(args_parser)
    args = args_parser.parse_args()

    inputs = {'ref.fa': args.reference,
              'ref.fa.fai': args.reference_index,
              'ref.fa.dict': args.reference_dict,
              'eval.vcf': args.eval_vcf,
              'comp.vcf': args.comp_vcf,
              's3_dir': args.output_dir,
              'uuid': None,
              'cpu_count': str(multiprocessing.cpu_count()),
              'ssec': None,
              'sudo': False}

    Job.Runner.startToil(Job.wrapJobFn(batch_start, inputs), args)
