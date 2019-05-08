import luigi

import settings

from pipelines.pipelines_web import *


class WebPipelinesRunner(luigi.WrapperTask):
    def requires(self):
        # stat.gov.kz
        yield StatGovOKED(sourcefile="web_statgov_oked.json")
        yield StatGovKPVED(sourcefile="web_statgov_kpved.json")
        yield StatGovNVED(sourcefile="web_statgov_nved.json")
        yield StatGovKURK(sourcefile="web_statgov_kurk.json")
        yield StatGovMKEIS(sourcefile="web_statgov_mkeis.json")
        yield StatGovKATO(sourcefile="web_statgov_kato.json")
        yield StatGovCompanies(sourcefile="web_statgov_companies.json")

        # kgd.gov.kz
        yield KgdGovPseudoCompany(sourcefile="web_kgdgov_pseudo_company.json")
        yield KgdGovWrongAddress(sourcefile="web_kgdgov_wrong_address.json")
        yield KgdGovBankrupt(sourcefile="web_kgdgov_bankrupt.json")
        yield KgdGovInactive(sourcefile="web_kgdgov_inactive.json")
        yield KgdGovInvalidRegistration(sourcefile="web_kgdgov_invalid_registration.json")
        yield KgdGovViolationTaxCode(sourcefile="web_kgdgov_violation_tax_code.json")
        yield KgdGovTaxArrearsULOver150(sourcefile="web_kgdgov_tax_arrears_150.json")


if __name__ == '__main__':
    luigi.run()
