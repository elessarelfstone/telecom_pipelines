import os
import json
import shutil

from box import Box
from os import path

from core.core import HandlersFactory
from core.utils import Utils


class ExtractorFile():
    def extract(self, instance, target_files):
        data_format = Box(json.loads(instance.conf)).storage.data_format
        arch = Utils.get_archive_object(instance.file)
        file_path = path.abspath(path.dirname(instance.file))
        for f, f_new in zip(arch.namelist(), target_files):
            if Utils.ext(f) == data_format:
                arch.extract(f, file_path)
                old_path = path.join(file_path, f).replace('/', os.sep)
                shutil.move(old_path, f_new)
        return target_files

    @staticmethod
    def path(conf, archive):
        arch = Utils.get_archive_object(archive)
        name = Box(json.loads(conf)).name
        data_format = Box(json.loads(conf)).storage.data_format
        files = list()
        file_path = path.abspath(path.dirname(archive))
        for i, f in enumerate(arch.namelist()):
            if Utils.ext(f) == data_format:
                tmp_path = path.join(file_path, f).replace('/', os.sep)
                new_path = path.abspath(path.dirname(tmp_path))
                # files.append(os.path.join(new_path, "{}_{}.{}".format(name, i, data_format)))
                files.append(os.path.join(new_path, "{}_{}.{}".format(name, Utils.uuid(), data_format)))

        return files

#
# class ExtractorFiles():
#     def extract(self, instance, target):
#
#         data_format = Box(json.loads(instance.conf)).storage.data_format
#         arch = Utils.get_archive_object(instance.file)
#         for f, t in zip(arch.namelist(), target):
#             if Utils.ext(f) == data_format:
#                 arch.extract(f, instance.directory)
#                 copyfile(os.path.join(instance.directory, f).replace('/', '\\'), t)
#         return target
#
#     @staticmethod
#     def path(conf, archive):
#         archives = archive
#         files = list()
#         for i, f in enumerate(archives):
#             arch = Utils.get_archive_object(f)
#             name = Box(json.loads(conf)).name
#             data_format = Box(json.loads(conf)).storage.data_format
#             files_in_arch = list()
#             for j, fr in enumerate(arch.namelist()):
#                 if Utils.ext(fr) == data_format:
#                     files_in_arch.append(os.path.join(directory, "{}_{}_{}.{}").format(name, i, j, data_format))
#             return files.extend(files_in_arch)
#
#         return files


HandlersFactory.register("extract_file", ExtractorFile)
