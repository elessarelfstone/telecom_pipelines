from pipelines.pipeline_download import DownloadFile

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


class StatGovKATO(DownloadFile):
    pass


class StatGovCompanies(DownloadFile):
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




