import pysftp
import logging
import os

class SftpHandler(pysftp.Connection):
    def __init__(self, host, port, user, password, path):
        # Инициализируем родительский класс (pysftp.Connection)
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        super().__init__(
            host=host,
            username=user,
            password=password,
            port=port,
            cnopts=cnopts
        )
        logging.info("Connection successful")
        self.path = path
        

    def get_meta(self, path=None):
        # get files meta from sftp
        if path is None:
            path = self.path

        sftp_meta = {}
        stack = [path]

        while stack:
            current_path = stack.pop()
            try:
                for entry in self.listdir(current_path):
                    entry_path = f"{current_path}/{entry}"
                    if self.isdir(entry_path):
                        stack.append(entry_path)
                    else:
                        file_attrs = self.stat(entry_path)
                        sftp_meta[entry_path] = {
                            "size": file_attrs.st_size,
                            "last_mod": file_attrs.st_mtime
                        }
            except Exception as e:
                logging.error(f"Error while retrieving metadata for {current_path}: {e}")

        return sftp_meta
    

    def sftp_to_local(self, files_to_upload, local_dwnld_dir):
        if not files_to_upload:
            logging.info("Nothing to upload, files_to_upload list is empty.")
            return 
        
        if not os.path.exists(local_dwnld_dir):
            os.makedirs(local_dwnld_dir)
            logging.info(f"Created local directory: {local_dwnld_dir}")

        for path in files_to_upload:
            local_file_path = os.path.join(local_dwnld_dir, os.path.basename(path))
            try: 
                logging.info(f"Starting download from {path} to {local_file_path}")
                self.get(path, local_file_path)
                logging.info(f"Downloaded {path} to {local_file_path}")
            except Exception as e:
                logging.error(f"Failed to download {path}: {e}")
