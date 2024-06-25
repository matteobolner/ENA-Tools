from ftplib import FTP
import pandas as pd
import os
from pathlib import Path
from urllib.parse import urlparse
import hashlib


class EnaFtp:
    """
    Access and download files from ENA FTP
    """

    def __init__(self, project, sample, run, urls, md5):
        self.project = project
        self.sample = sample
        self.run = run
        self.urls = urls.split(";")
        self.md5 = md5.split(";")

    def setup_ftp(self, root="ftp.sra.ebi.ac.uk", main_dir="vol1/fastq/"):
        ftp = FTP(root)
        ftp.login()
        ftp.cwd(main_dir)
        return ftp

    def get_metadata(self):
        metadata_dict = {
            "project": self.project,
            "sample": self.sample,
            "run": self.run,
        }
        return metadata_dict

    def byte_to_gigabyte(self, size):
        return size / 1e9

    def build_folder_structure(self, output_dir, include_project=False):
        if include_project == True:
            outpath = os.path.join(output_dir, self.project, self.sample)
        else:
            outpath = os.path.join(output_dir, self.sample)
        try:
            path = Path(outpath).mkdir(parents=True)
            return outpath
        except:
            return outpath

    def files_paths(self):
        """
        return file path on FTP server
        """
        files_path_dict = {}
        for url in self.urls:
            url = urlparse(url)
            filename = os.path.splitext(url.path)[0]
            filename = os.path.splitext(filename)[0]
            filename = os.path.basename(filename)
            ftp_path = "/".join(url.path.split("/")[3::])
            files_path_dict[filename] = ftp_path
        return files_path_dict

    def files_sizes(self):
        """
        return file size in GB
        """
        ftp = self.setup_ftp()
        files = self.files_paths()
        size_dict = {}
        for file in files.keys():
            size_dict[file] = self.byte_to_gigabyte(ftp.size(files[file]))
        return size_dict

    def files_md5(self):
        """
        return file md5
        """
        files = self.files_paths()
        md5_dict = {}
        for url, md5 in zip(self.urls, self.md5):
            url = urlparse(url)
            filename = os.path.splitext(url.path)[0]
            filename = os.path.splitext(filename)[0]
            filename = os.path.basename(filename)
            md5_dict[filename] = md5
        return md5_dict

    def checksum(self, filename, chunk_num_blocks=128):
        h = hashlib.md5()
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_num_blocks * h.block_size), b""):
                h.update(chunk)
        return h.hexdigest()

    def download_fastq(self, output_dir, filename):
        ftp = self.setup_ftp()
        ftp_path = self.files_paths()[filename]
        path = self.build_folder_structure(output_dir=output_dir, include_project=False)
        fullpath = os.path.join(path, filename + ".fastq.gz")
        if os.path.isfile(fullpath):
            local_md5 = self.checksum(fullpath)
            remote_md5 = self.files_md5()[filename]
            if local_md5 == remote_md5:
                print("%s has already been downloaded with no errors" % filename)
                ftp.quit()
                return fullpath
            else:
                print("%s is present but with a different md5:" % filename)
                print("{0} vs {1}".format(local_md5, remote_md5))
                ftp.quit()
                return fullpath
        else:
            with open(fullpath, "wb") as fp:
                ftp.retrbinary("RETR %s" % ftp_path, fp.write)
            ftp.quit()
            return fullpath
