from pipelines.pipeline_download import DownloadFile
from pipelines.pipeline_extract import ExtractFile, ExtractFiles
from pipelines.pipeline_xlsparse import ParseXLS, ParseXLSFromArchive, ParseXLSFromArchives
from pipelines.pipeline_ftp import CopyFromFileToFtp, CopyFromArchToFtp, CopyFromArchsToFtp, CopyFromParsingToFtp
from pipelines.pipeline_parsers import ParseFromWebToCsv

# statgov


class StatGovOKED(CopyFromFileToFtp):
    pass


class StatGovKPVED(CopyFromFileToFtp):
    pass


class StatGovNVED(CopyFromFileToFtp):
    pass


class StatGovKURK(CopyFromFileToFtp):
    pass


class StatGovMKEIS(CopyFromFileToFtp):
    pass


class StatGovKATO(CopyFromArchToFtp):
    pass


class StatGovCompanies(CopyFromArchsToFtp):
    pass


# kgdgov


class KgdGovPseudoCompany(CopyFromFileToFtp):
    pass


class KgdGovWrongAddress(CopyFromFileToFtp):
    pass


class KgdGovBankrupt(CopyFromFileToFtp):
    pass


class KgdGovInactive(CopyFromFileToFtp):
    pass


class KgdGovInvalidRegistration(CopyFromFileToFtp):
    pass


class KgdGovViolationTaxCode(CopyFromFileToFtp):
    pass


class KgdGovTaxArrearsULOver150(CopyFromFileToFtp):
    pass


class KgdGovRefinanceRate(CopyFromFileToFtp):
    pass


#datagov

class DataGovAddresses(CopyFromParsingToFtp):
    pass
