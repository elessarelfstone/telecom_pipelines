import luigi

import settings

from core.notification import LuigiTelegramNotification
from pipelines.pipelines_web import *


class MyNotification(LuigiTelegramNotification):
    def on_success(self, task):
        super().on_success(task)
        self.notify()
        self._succeeded_tasks = []

    def on_failure(self, task, exception):
        super().on_failure(task, exception)
        self.notify()
        self._failed_tasks = []


class WebPipelinesRunner(luigi.WrapperTask):
    def requires(self):
        # stat.gov.kz
        yield StatGovOKED(sourcefile="web_statgov_oked.json", jobfile="to_csv.json")
        yield StatGovKPVED(sourcefile="web_statgov_kpved.json", jobfile="to_csv.json")
        yield StatGovNVED(sourcefile="web_statgov_nved.json", jobfile="to_csv.json")
        yield StatGovKURK(sourcefile="web_statgov_kurk.json", jobfile="to_csv.json")
        yield StatGovMKEIS(sourcefile="web_statgov_mkeis.json", jobfile="to_csv.json")
        yield StatGovKATO(sourcefile="web_statgov_kato.json", jobfile="to_csv.json")
        yield StatGovCompanies(sourcefile="web_statgov_companies.json", jobfile="to_csv.json")

        # kgd.gov.kz
        yield KgdGovPseudoCompany(sourcefile="web_kgdgov_pseudo_company.json", jobfile="to_csv.json")
        yield KgdGovWrongAddress(sourcefile="web_kgdgov_wrong_address.json", jobfile="to_csv.json")
        yield KgdGovBankrupt(sourcefile="web_kgdgov_bankrupt.json", jobfile="to_csv.json")
        yield KgdGovInactive(sourcefile="web_kgdgov_inactive.json", jobfile="to_csv.json")
        yield KgdGovInvalidRegistration(sourcefile="web_kgdgov_invalid_registration.json", jobfile="to_csv.json")
        yield KgdGovViolationTaxCode(sourcefile="web_kgdgov_violation_tax_code.json", jobfile="to_csv.json")
        yield KgdGovTaxArrearsULOver150(sourcefile="web_kgdgov_tax_arrears_150.json", jobfile="to_csv.json")
        yield KgdGovRefinanceRate(sourcefile="web_kgdgov_refinance_rate.json", jobfile="to_csv.json")
        yield KgdGovMrp(sourcefile="web_kgdgov_mrp.json", jobfile="to_csv.json")
        yield KgdGovMzp(sourcefile="web_kgdgov_mzp.json", jobfile="to_csv.json")

        #data.gov.kz
        yield DataGovAddresses(sourcefile="web_datagov_addresses.json", jobfile="to_csv.json")
        yield DataGovUnemploymentRate(sourcefile="web_datagov_unemployment_rate.json", jobfile="to_csv.json")
        yield DataGovUnemploymentPercentRate(sourcefile="web_datagov_unemployment_percent_rate.json", jobfile="to_csv.json")


lgtg = MyNotification('711584403:AAGLj7MAly4dqlhvSj3Ymr9tmGOXeURPbcw', [498912844])
lgtg.set_handlers()

if __name__ == '__main__':
        luigi.run()
