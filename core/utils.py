from urllib.parse import urlparse


class Utils():
    @staticmethod
    def base_url(url):
        parsed_uri = urlparse(url)
        result = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        return result
