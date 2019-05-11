from pipelines.pipeline_download import DownloadFile
from pipelines.pipeline_extract import ExtractFile, ExtractFiles

# statgov

class StatGovOKED(DownloadFile):
    pass


class StatGovKPVED(DownloadFile):
    pass


class StatGovNVED(DownloadFile):
    pass


class StatGovKURK(DownloadFile):
    pass


class StatGovMKEIS(DownloadFile):
    pass

#
# class StatGovKATO(DownloadFile):
#     pass

class StatGovKATO(ExtractFile):
    pass


class StatGovCompanies(ExtractFiles):
    pass

# kgdgov


class KgdGovPseudoCompany(DownloadFile):
    pass


class KgdGovWrongAddress(DownloadFile):
    pass


class KgdGovBankrupt(DownloadFile):
    pass


class KgdGovInactive(DownloadFile):
    pass


class KgdGovInvalidRegistration(DownloadFile):
    pass


class KgdGovViolationTaxCode(DownloadFile):
    pass


class KgdGovTaxArrearsULOver150(DownloadFile):
    pass




