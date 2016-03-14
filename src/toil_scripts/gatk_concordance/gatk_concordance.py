#!/usr/bin/env python2.7

import argparse
import multiprocessing
import os
import subprocess

from toil.job import Job

from toil_scripts import download_from_s3_url
from toil_scripts.batch_alignment.bwa_alignment import upload_to_s3, docker_call

def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--reference', required=True, help="Reference Genome URL")
    parser.add_argument('-i', '--reference_index', required=True, help="Reference Genome index (.fai) URL")
    parser.add_argument('-e', '--eval_vcf', required=True, help="VCF file URL to evaluate")
    parser.add_argument('-c', '--comp_vcf', required=True, help="VCF file URL to compare against")
    parser.add_argument('-o', '--output_dir', required=True, help="Output directory S3 URL")
    return parser


# copied from germline.py
def download_url_c(job, url, filename):
    work_dir = job.fileStore.getLocalTempDir()
    file_path = os.path.join(work_dir, filename)
    if not os.path.exists(file_path):
        if url.startswith('s3:'):
            download_from_s3_url(file_path, url)
        else:
            try:
                subprocess.check_call(['curl', '-fs', '--retry', '5', '--create-dir', url, '-o', file_path])
            except subprocess.CalledProcessError as cpe:
                raise RuntimeError(
                    '\nNecessary file could not be acquired: %s. Got error "%s". Check input URL' % (url, cpe))
            except OSError:
                raise RuntimeError('Failed to find "curl". Install via "apt-get install curl"')
    assert os.path.exists(file_path)
    return job.fileStore.writeGlobalFile(file_path)


# copied from germline.py
def read_from_filestore_c(job, work_dir, ids, *filenames):
    for filename in filenames:
        if not os.path.exists(os.path.join(work_dir, filename)):
            job.fileStore.readGlobalFile(ids[filename], os.path.join(work_dir, filename))


def batch_start(job, input_args):
    shared_files = ['ref.fa', 'ref.fa.fai', 'eval.vcf', 'comp.vcf']
    shared_ids = {}
    for file_name in shared_files:
        url = input_args[file_name]
        shared_ids[file_name] = job.addChildJobFn(download_url_c, url, file_name).rv()
    job.addFollowOnJobFn(concordance, shared_ids, input_args)
    

def concordance(job, shared_ids, input_args):
    """
    Evaluate concordance between two VCFs with GATK.
    """
    work_dir = job.fileStore.getLocalTempDir()
    input_files = ['ref.fa', 'ref.fa.fai', 'eval.vcf', 'comp.vcf']
    read_from_filestore_c(job, work_dir, shared_ids, *input_files)

    eval_name = input_args['eval.vcf'].split('/')[-1]
    comp_name = input_args['comp.vcf'].split('/')[-1]
    output = '%s_vs_%s.concordance' % (eval_name, comp_name)
    command = ['-T', 'GenotypeConcordance',
               '-R', 'ref.fa',
               '--eval', 'eval.vcf',
               '--comp', 'comp.vcf',
               '-o', output
    ]
    try:
        docker_call(work_dir = work_dir,
                    tool_parameters = command,
                    tool = 'quay.io/ucsc_cgl/gatk',
                    sudo = input_args['sudo'])
    except:
        sys.stderr.write("Running concordance with %s in %s failed." % (
            " ".join(command), work_dir))
        raise

    upload_to_s3(work_dir, input_args, output)


if __name__ == '__main__':
    args_parser = build_parser()
    Job.Runner.addToilOptions(args_parser)
    args = args_parser.parse_args()

    inputs = {'ref.fa': args.reference,
              'ref.fa.fai': args.reference_index,
              'eval.vcf', args.eval_vcf,
              'comp.vcf', args.comp_vcf,
              's3_dir', args.output_dir,
              'uuid': None,
              'cpu_count': str(multiprocessing.cpu_count()),
              'ssec': None,
              'sudo': False}

    Job.Runner.startToil(Job.wrapJobFn(batch_start, inputs), args)
