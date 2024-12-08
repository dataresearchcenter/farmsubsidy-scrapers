



base_url = 'https://www.transparenz.at/export/Export_%s.zip'

def download(url):
    pass

def run(year):
    url = base_url % year
    print('Downloading from %s' % url)
    return base_url
