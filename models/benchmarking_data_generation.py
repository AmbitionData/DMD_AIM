from __future__ import division

import pandas as pd
import utils
import datetime
from personas import get_personas


def get_quarters(dt):
    """
    Map dates to quarter (this is weird and specific, need to generalize)

    :param datetime dt: timestamp with date extracted
    """
    if dt < datetime.date(2017, 5, 1):
        return 'q1'
    elif dt < datetime.date(2017, 8, 1):
        return 'q2'
    else:
        return 'q3'


def categorize_frequency(n):
    if n < 1:
        return 'Less than 1 visit a month'
    elif n <= 10:
        return '1-10 times per month'
    else:
        return 'More than 10x per month'


def get_use_case(events, three=True):
    """
    Assign use cases to sessions

    :param df events: events file with urls categorized
    :param boolean three: True if getting personas, False if wanting more use cases
    """
    if three:
        publisher_tools = ['Publisher', 'Reference Tool', 'Medical Education', 'Multi-Channel Marketing']
        pharma = ['Pharma', 'Med Device']
        social = ['Professional Social', 'Medical Association', 'Recruiter']

        events['use_case'] = ''
        events.loc[events.site_category.isin(publisher_tools), 'use_case'] = 'publications_ed_tools'
        events.loc[events.site_category.isin(pharma), 'use_case'] = 'pharma_device'
        events.loc[events.site_category.isin(social), 'use_case'] = 'professional_social_media'
        events.loc[events.use_case == '', 'use_case'] = 'other'
    else:
        use_cases = {
            'publication_research': ['Publisher'],
            'education_tools': ['Reference Tool', 'Medical Education', 'Multi-Channel Marketing'],
            'pharma': ['Pharma', 'Med Device'],
            'social_professional': ['Professional Social', 'Medical Association', 'Recruiter']
        }

        for u in use_cases:
            events.loc[events.site_category.isin(use_cases[u]), 'use_case'] = u
        events.loc[events.use_case == '', 'use_case'] = 'other'
    return events


def aggregate_sessions(visitors=None,
                       events=None,
                       output_file=None,
                       use_case_truncated=True,
                       personas=True):
    """
    Group events data by sessions, tag use_cases, extract personas if desired

    :param df visitors: visitors data, if not initialized will load from raw data
    :param df events: events data, if not initlalized will load from raw data
    :param str output_file: optional, filepath if writing to tableau
    :param boolean use_case_truncated: True if exracting personas, False if wanting more use cases
    :param boolean personas: True if wanting personas, False if not
    """

    if visitors is None or events is None:
        # load data
        visitors, events, devices, url_categories = utils.load_data(event_categories=True)

    ev = get_use_case(events, three=use_case_truncated)
    ev['quarter'] = ev.timestamp.apply(lambda x: get_quarters(x.date()))
    sessions = ev.groupby('session_id').event_id.count().reset_index()
    sessions = sessions.rename(columns={'event_id': 'page_views'})
    event_sessions = events.drop_duplicates('session_id')
    event_sessions = pd.merge(event_sessions, sessions, on='session_id')
    event_sessions = pd.merge(visitors, event_sessions, on='dg_id')

    tableau_cols = ['timestamp', 'dg_id', 'npi_number', 'primary_specialty', 'primary_specialty_group',
                    'site_category', 'site_sub_category', 'disease_category',
                    'disease', 'pharma_firm', 'use_case', 'page_views']
    if personas:
        personas = get_personas(event_sessions, visitors)
        event_sessions = pd.merge(event_sessions, personas[['dg_id', 'persona']], on='dg_id')
        tableau_cols.append('persona')

    if output_file is not None:
        tableau_sessions = event_sessions[tableau_cols]
        tableau_sessions.to_csv(output_file, index=False)

    return event_sessions


def npi_level_summary_statistics(output_file=None, npis=None, personas=True):
    """
    Pull benchmarks from NPI inputs

    :param str output_file: optional, include if writing to file
    :param list npis: list of NPI numbers we want to extract benchmarks from
    :param boolean personas: flag if we want personas extracted (tableau does)
    """

    # load data
    visitors, events, devices, url_categories = utils.load_data(event_categories=True)

    # if given a list of users, pull in those. otherwise, use all available data
    if npis:
        v = visitors[visitors.npi_number.isin(npis)]
    else:
        v = visitors[visitors.exclude == 0]
    ev = pd.merge(v, events, on='dg_id')
    if len(ev) == 0:
        print 'target list has no web activity'
        return

    # check which users have web activity
    event_dgids = set(ev.dg_id)
    v['has_event'] = v.dg_id.apply(lambda x: 1 if x in event_dgids else 0)

    # check frequency of web activity (timeframe denominator starts with the earliest date the user has activity)
    max_dt = max(events.timestamp).date()
    ev['dt'] = ev.timestamp.apply(lambda x: x.date())
    ev['quarter'] = ev.dt.apply(lambda x: get_quarters(x))
    freq = ev.drop_duplicates(subset=['dg_id', 'dt']).groupby(['dg_id']).dt.agg(['min', 'max', 'count']).reset_index()
    freq['sessions_per_month'] = freq.apply(lambda x: x['count'] / (max(1, (max_dt - x['min']).days) / 30), axis=1)
    freq['frequency_category'] = freq.sessions_per_month.apply(lambda x: categorize_frequency(x))
    v = pd.merge(v, freq[['dg_id', 'sessions_per_month', 'frequency_category']], how='left')

    # separate frequency by quarter
    freqq = ev.drop_duplicates(subset=['dg_id', 'dt']).groupby(['dg_id', 'quarter']).dt.agg(['min', 'max', 'count']).reset_index()
    freqq['sessions_per_month'] = freqq.apply(lambda x: x['count'] / (max(1, (max_dt - x['min']).days) / 30), axis=1)
    freqq['frequency_category'] = freqq.sessions_per_month.apply(lambda x: categorize_frequency(x))
    freqq = freqq.pivot(index='dg_id', columns='quarter', values='frequency_category').reset_index()
    freqq.columns = ['dg_id', 'frequency_category_q1', 'frequency_category_q2']
    v = pd.merge(v, freqq, on='dg_id', how='left')

    # separate frequency by month
    ev['month'] = ev.timestamp.apply(lambda x: x.strftime('%B'))
    ev['year'] = ev.timestamp.apply(lambda x: x.year)

    freqm = ev.drop_duplicates(subset=['dg_id', 'dt'])\
        .groupby(['dg_id', 'month', 'year']).dt.agg(['min', 'max', 'count'])\
        .reset_index().rename(columns={'count': 'sessions_per_month'})

    freqm['frequency_category'] = freqm.sessions_per_month.apply(lambda x: categorize_frequency(x))
    freqm = freqm.pivot(index='dg_id', columns='month', values='frequency_category').reset_index()
    freqm = freqm.fillna('Less than 1 visit a month')
    freqm.columns = ['dg_id'] + ['frequency_category_{}'.format(x.lower()) for x in freqm.columns[1:]]
    v = pd.merge(v, freqm, on='dg_id', how='left')

    # grab avg number of urls per session
    urls_per_session = ev.groupby(['dg_id', 'session_id']).url.nunique().reset_index()\
        .groupby('dg_id').url.mean().reset_index()
    urls_per_session = urls_per_session.rename(columns={'url': 'urls_per_session'})
    v = pd.merge(v, urls_per_session, on='dg_id', how='left')

    tableau_columns = [
        'dg_id',
        'identity_type',
        'professional_designation',
        'npi_number',
        'primary_specialty',
        'primary_specialty_group',
        'birth_year',
        'grad_year',
        'gender',
        'has_event',
        'sessions_per_month',
        'urls_per_session'
    ]

    tableau_columns += [x for x in v.columns if 'frequency' in x]

    if personas:
        event_sessions = aggregate_sessions(v, ev, use_case_truncated=True)
        personas = get_personas(event_sessions, v)
        v = pd.merge(v, personas[['dg_id', 'persona']], on='dg_id', how='left')
        tableau_columns.append('persona')

    if output_file is not None:
        v[tableau_columns].to_csv(output_file, index=False)

    return v[tableau_columns]
