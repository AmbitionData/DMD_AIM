
from __future__ import division

def load_data():
    import pandas as pd
    # load
    visitors = pd.read_csv('../data/visitors.csv')
    events = pd.read_csv('../data/web_events.csv', parse_dates=['timestamp'])
    devices = pd.read_csv('../data/machine_fingerprints.csv')
    url_categories = pd.read_csv('../data/disease_mapping_cbg_newURLs.csv')

    # cleanup
    if len(url_categories.columns) > 6:
        url_categories = url_categories[url_categories.columns[:6]]
    url_categories.columns = ['url', 'site_category', 'site_sub_category', 'disease_category', 'disease', 'pharma_firm']
    url_categories = url_categories.dropna(subset=['url'])

    visitors.columns = [x.strip() for x in visitors.columns]
    events.columns = [x.strip() for x in events.columns]

    # exclude bots
    bot_dgids = set(visitors[visitors.suspected_bot == 'Y'].dg_id)
    events = events[~events.dg_id.isin(bot_dgids)]

    visitors['exclude'] = 0
    # found weird cases where one npi number is mapped to multiple dg_id, flagging for now
    dups = set(visitors.groupby('npi_number').dg_id.count().loc[lambda x: x > 1].index)
    visitors.loc[visitors.suspected_bot == 'Y', 'exclude'] = 1
    visitors.loc[visitors.npi_number.isin(dups), 'exclude'] = 1

    return visitors, events, devices, url_categories


def clean_url(url, strip=False):
    import re
    #strip html
    url = url.lower()
    url = url.replace('http://', '')
    url = url.replace('https://', '')

    #strip www.
    url = url.replace('www.', '')

    #strip backslash at end
    if url[-1] == '/':
        url = url[:-1]

    if strip:
        if 'hcp' in re.split('[^a-zA-Z]', url):
            url = url.split('.')[0] + '/hcp'
        url = url.split('.')[0]
    return url.strip()


def match_urls(url1, url2):
    if url1 == url2:
        return 1
    if '/' in url1 and '/' in url2:
        if url1.split('/')[1].split('/')[0] == url2.split('/')[1].split('/')[0]:
            return 1
        else:
            return 0
    else:
        return 0

def categorize_events(events, url_categories):
    import pandas as pd

    url_categories['stripped_url'] = url_categories.url.apply(lambda x: clean_url(x, strip=True))
    url_categories['clean_url'] = url_categories.url.apply(lambda x: clean_url(x))
    url_categories = url_categories.drop_duplicates(subset=['clean_url'])

    event_urls = events.groupby('url').session_id.nunique().sort_values(ascending=False).reset_index()
    event_urls['stripped_url'] = event_urls.url.apply(lambda x: clean_url(x, strip=True))
    event_urls['clean_url'] = event_urls.url.apply(lambda x: clean_url(x))

    event_urls = event_urls.rename(columns={'url':'event_url', 'clean_url':'event_clean_url'})
    m = pd.merge(event_urls, url_categories, on='stripped_url', how='left')

    bad_roots = url_categories.groupby('stripped_url').url.count().loc[lambda x: x>1].reset_index()
    bad_roots.columns = ['stripped_url', 'cnt']

    fix_me = pd.merge(m, bad_roots, on='stripped_url')
    fix_me['match'] = fix_me.apply(lambda x: match_urls(x.clean_url, x.event_clean_url), axis=1)

    m = m[~m.event_url.isin(fix_me.event_url)]
    events_category = pd.merge(events, m, left_on='url', right_on='event_url')
    events_category = events_category.append(pd.merge(events, fix_me[fix_me.match==1],left_on='url', right_on='event_url' ))

    print 'match rate: {}%'.format(round(len(events_category)/len(events),2)*100)
    return events_category