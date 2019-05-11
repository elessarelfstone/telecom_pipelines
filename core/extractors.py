import os
import json
import shutil

from box import Box
from os import path

from core.core import HandlersFactory
from core.utils import Utils


class ExtractorFile():
    @staticmethod
    def extract(instance, target_files):
        data_format = Box(json.loads(instance.conf)).storage.data_format
        arch_obj = Utils.get_archive_object(instance.file)
        file_path = path.abspath(path.dirname(instance.file))
        for arch_file, new_path in zip(arch_obj.namelist(), target_files):
            if Utils.ext(arch_file) == data_format:
                arch_obj.extract(arch_file, file_path)
                tmp_path = path.join(file_path, arch_file).replace('/', os.sep)
                shutil.move(tmp_path, new_path)
        return target_files

    @staticmethod
    def path(conf, directory):
        files_num = Box(json.loads(conf)).storage.data_files_num
        name = Box(json.loads(conf)).name
        data_format = Box(json.loads(conf)).storage.data_format
        files = list()
        for i in range(files_num):
            files.append(os.path.join(directory, "{}_{}.{}".format(name, i, data_format)))
        return files


class ExtractorFiles():
    @staticmethod
    def extract(instance, target):
        targets = target
        # print(targets)
        archives = instance.file
        i = -1
        for arch in archives:
            arch_obj = Utils.get_archive_object(arch)
            data_format = Box(json.loads(instance.conf)).storage.data_format
            file_path = path.abspath(path.dirname(arch))
            for file in arch_obj.namelist():
                if Utils.ext(file) == data_format:
                    i += 1
                    arch_obj.extract(file, file_path)
                    old_path = path.join(file_path, file).replace('/', os.sep)
                    shutil.move(old_path, targets[i])
        return targets

    @staticmethod
    def path(conf, directory):
        files_num = Box(json.loads(conf)).storage.data_files_num
        archives_num = Box(json.loads(conf)).storage.data_archives_num
        name = Box(json.loads(conf)).name
        data_format = Box(json.loads(conf)).storage.data_format
        files = list()
        for i in range(archives_num):
            for j in range(files_num):
                files.append(path.join(directory, "{}_{}_{}.{}".format(name, i, j, data_format)))
        return files




HandlersFactory.register("extract_file", ExtractorFile)
HandlersFactory.register("extract_files", ExtractorFiles)
