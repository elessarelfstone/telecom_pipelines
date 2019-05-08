from urllib.parse import urlparse


class Utils():
    @staticmethod
    def read_file(file):
        with open(file, "r", encoding="utf8") as f:
            result = f.read()
        return result

    @staticmethod
    def base_url(url):
        parsed_uri = urlparse(url)
        result = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        return result


