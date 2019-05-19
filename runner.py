import luigi

import web_pipelines_runner as wpr


class Runner(luigi.WrapperTask):
    def requires(self):
        yield wpr.WebPipelinesRunner()


if __name__ == '__main__':
    luigi.run()
