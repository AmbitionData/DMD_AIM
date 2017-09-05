from __future__ import division

import pandas as pd
import utils
import datetime


def process_unknowns(filepath):
    """
    DMD provided us with a file containing unknown web sessions by date by domain.
    This ingests and cleans that file

    :param str filepath: path to the unknown data file (specified in utils right now)
    """
    # grab unknowns
    unk = pd.read_csv(filepath, parse_dates=['date'])
    unk = unk.dropna(subset=['domain'])

    # since we were only given these at the domain level, will be sufficient to strip urls after .com
    unk['stripped_url'] = unk.domain.apply(lambda x: utils.clean_url(x, strip=True))
    unk['dt'] = unk.date.apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))

    unk = unk.rename(columns={'sessions': 'unk_sessions'})
    unkd = unk.groupby(['stripped_url', 'dt']).unk_sessions.sum().reset_index()
    return unkd


def aggregate_pharma_activity(visitors, events, url_categories):
    """
    Aggregate pharma web sessions by url and category
    Determines how many authorized sessions occurred at that url on each date

    :param df visitors: visitors data
    :param df events: events data
    :param df url_categories: url categories provided by DMD
    """

    # bring in visitors, determine if authorized
    categorized_visits = pd.merge(visitors, events, on='dg_id')
    categorized_visits['authorized'] = categorized_visits.identity_type.apply(lambda x: 1 if x == 'AUT' else 0)

    # isolate pharma companies
    pharma = categorized_visits[categorized_visits.site_category == 'Pharma']
    pharma['dt'] = pharma.timestamp.apply(lambda x: x.date())
    pharma.dt = pd.to_datetime(pharma.dt)
    pharma_activity = pharma.drop_duplicates(subset=['dg_id', 'session_id', 'category_url']).groupby(['category_url', 'dt']).authorized.agg(['sum', 'count']).reset_index()
    pharma_activity.columns = ['url', 'dt', 'authorized_visits', 'identified_visits']
    pharma_activity = pd.merge(url_categories, pharma_activity, on='url').fillna('')
    return pharma_activity


def monthly_aut_overall(visitors=None, events=None, url_categories=None, output_file=None):
    """
    For each pharma company, calculate the number of authenticated sessions vs total sessions per month

    :param str output_file: specify if df is to be written to csv for tableau
    """
    if visitors is None or events is None or url_categories is None:
        # load data
        visitors, events, devices, url_categories = utils.load_data(event_categories=True)
    unkd = process_unknowns(utils.path_to_unknowns)

    pharma_activity = aggregate_pharma_activity(visitors, events, url_categories)
    # merge
    pharma_merged = pd.merge(pharma_activity, unkd, on=['stripped_url', 'dt'], how='left')
    pharma_merged['month'] = pharma_merged.dt.apply(lambda x: x.month)
    pharma_merged['total_visits'] = pharma_merged.apply(lambda x: x.identified_visits + x.unk_sessions, axis=1)
    pharma_merged['pct_authorized'] = pharma_merged.apply(lambda x: x.authorized_visits / x.total_visits, axis=1)
    # keep useful columns
    pharma_merged = \
        pharma_merged[['url_category_id', 'dt', 'url', 'site_sub_category',
                       'month', 'disease_category', 'disease', 'pharma_firm',
                       'authorized_visits', 'identified_visits', 'unk_sessions',
                       'total_visits', 'dt_first_view', 'pct_authorized']]
    # group by month
    pharma_merged = pharma_merged.groupby(['url_category_id', 'month'])[['authorized_visits',
                                                                         'identified_visits',
                                                                         'unk_sessions',
                                                                         'total_visits']].sum().reset_index()
    pharma_merged = pd.merge(url_categories, pharma_merged, on='url_category_id')
    pharma_merged['pct_authorized'] = pharma_merged.apply(lambda x: x.authorized_visits / x.total_visits, axis=1)

    if output_file:
        pharma_merged.to_csv(output_file, index=False)

    return pharma_merged
