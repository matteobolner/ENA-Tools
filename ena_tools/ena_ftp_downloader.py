#!/usr/bin/python3

import os
import pandas as pd
import argparse
from ena import EnaFtp

from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool


def main(input_df, output_dir, threads=1, dryrun=False, report_errors=None):
    """
    Read csv containing metadata, split in rows and for each row download reads
    """
    df = pd.read_table(input_df)

    rows_list = [df.loc[i] for i in df.index]

    if threads == 1:
        download_errors_list = []
        for row in rows_list:
            error_list = download_run(row)
            download_errors_list.append(error_list)
    else:
        pool = ThreadPool(threads)
        download_errors_list = pool.map(download_run, rows_list)
        pool.close()
        pool.join()

    if report_errors:
        download_errors = [j for i in download_errors_list for j in i]
        with open(args["report_errors"], "w") as f:
            for i in download_errors:
                f.write(i)
                f.write("\n")
    return ()


def download_run(row):
    # TODO:ADD to func args -> project, sample, run, urls, md5
    download_errors = []
    run_data = EnaFtp(
        project=row["study_accession"],
        sample=row["sample_accession"],
        run=row["run_accession"],
        urls=row["fastq_ftp"],
        md5=row["fastq_md5"],
    )
    filenames = run_data.files_paths().keys()

    if args.dryrun:
        print(run_data.run, (str(sum(run_data.files_sizes().values())) + " GB"))
    else:
        for file in filenames:
            downloaded_run = run_data.download_fastq(
                output_dir=args.output, filename=file
            )
            local_md5 = run_data.checksum(downloaded_run)
            remote_md5 = run_data.files_md5()[file]

            if local_md5 == remote_md5:
                continue
            else:
                download_errors.append(file)
                raise ValueError(f"MD5 checksum does not match")
    return download_errors


def configure_args():
    """
    Setup args
    """
    parser = argparse.ArgumentParser(description="Download data from ENA FTP server")

    parser.add_argument(
        "-i",
        "--input",
        help="File containing sample and reads information",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Name of the folder in which all data will be saved",
        required=True,
    )
    parser.add_argument(
        "-n",
        "--threads",
        type=int,
        help="Number of threads to use for parallelization",
        required=False,
        default=1,
    )
    parser.add_argument(
        "-d",
        "--dryrun",
        help="Dry run, show which runs will be downloaded and their file size",
        required=False,
        action="store_true",
    )
    # parser.add_argument('-p','--progress', help='Show download progress', required=False, action='store_true')
    parser.add_argument(
        "-r",
        "--report_errors",
        help="Output file report containing failed downloads",
        required=False,
    )
    return parser


if __name__ == "__main__":
    parser = configure_args()
    args = parser.parse_args()
    main(
        input_df=args.input,
        output_dir=args.output,
        threads=args.threads,
        dryrun=args.dryrun,
        report_errors=args.report_errors,
    )
