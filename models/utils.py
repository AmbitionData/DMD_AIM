
from __future__ import division

# define paths to files
path_to_visitors = '../data/visitors_20170925.csv'
path_to_events = '../data/web_events_20170925.csv'
path_to_devices = '../data/machine_fingerprints.csv'
path_to_url_categories = '../data/url_categories_final_20170822.csv'
path_to_unknowns = '../data/unqualified_traffic_summary.csv'

def load_data(event_categories=False):
    import pandas as pd
    # load
    visitors = pd.read_csv(path_to_visitors)
    events = pd.read_csv(path_to_events, parse_dates=['timestamp'])
    devices = pd.read_csv(path_to_devices)
    url_categories = pd.read_csv(path_to_url_categories)

    # cleanup
    visitors.columns = [x.strip() for x in visitors.columns]
    events.columns = [x.strip() for x in events.columns]
    events = events.rename(columns={'Unnamed: 0':'event_id'})
    if 'event_id' not in events.columns:
        events['event_id'] = range(len(events))
    visitors = visitors.rename(columns={'Unnamed: 0': 'visitor_id'})

    # add id to url_categories
    url_categories['url_category_id'] = range(len(url_categories))

    # exclude bots
    if 'suspected_bot' not in visitors.columns:
        visitors['suspected_bot'] = 'N'
    bot_dgids = set(visitors[visitors.suspected_bot == 'Y'].dg_id)
    events = events[~events.dg_id.isin(bot_dgids)]

    visitors['exclude'] = 0
    # found weird cases where one npi number is mapped to multiple dg_id, flagging for now
    dups = set(visitors.groupby('npi_number').dg_id.count().loc[lambda x: x > 1].index)
    visitors.loc[visitors.suspected_bot == 'Y', 'exclude'] = 1
    visitors.loc[visitors.npi_number.isin(dups), 'exclude'] = 1

    if event_categories:
        events = categorize_events(events, url_categories)

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
            # 2 special cases with hcp (ug)
            if 'yervoy' in url:
                url = 'yervoy/hcp'
            elif 'cabometyx' in url:
                url = 'cabometyx/hcp'
            else:
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

    event_urls = events[['url']].drop_duplicates().rename(columns={'url':'event_url'})
    event_urls['stripped_url'] = event_urls.event_url.apply(lambda x: clean_url(x, strip=True))
    event_urls['event_clean_url'] = event_urls.event_url.apply(lambda x: clean_url(x))

    m = pd.merge(event_urls, url_categories, on='stripped_url', how='left')

    bad_roots = url_categories.groupby('stripped_url').url.count().loc[lambda x: x>1].reset_index()
    bad_roots.columns = ['stripped_url', 'cnt']

    fix_me = pd.merge(m, bad_roots, on='stripped_url')
    fix_me['match'] = fix_me.apply(lambda x: match_urls(x.clean_url, x.event_clean_url), axis=1)
    fix_me = fix_me[fix_me.match==1][['url', 'event_url', 'site_category', 'site_sub_category', 'disease_category', 'disease', 'pharma_firm']]

    m = m[~m.event_url.isin(fix_me.event_url)][['url', 'event_url', 'site_category', 'site_sub_category', 'disease_category', 'disease', 'pharma_firm']]
    m = m.rename(columns={'url':'category_url', 'event_url':'url'})
    fix_me = fix_me.rename(columns={'url':'category_url', 'event_url':'url'})
    m = m.append(fix_me)

    events_category = pd.merge(events, m, on='url', how='left')

    #print 'match rate: {}%'.format(round(len(events_category)/len(events),2)*100)
    return events_category
