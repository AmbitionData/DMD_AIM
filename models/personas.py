from __future__ import division

import pandas as pd
import numpy as np
from collections import Counter



# https://en.wikipedia.org/wiki/Ternary_plot
def to_cartesian(a, b, c):
    """
    Convert ternary percentages to cartesian coordinates for easy plotting
    """
    x = .5 * ((2 * b) + c) / (a + b + c)
    y = (np.sqrt(3) / 2) * (c / (a + b + c))
    return (x, y)


def coordinates_to_persona(x, y):
    """
    Classify cartesian coordinates into personas
    """
    if y > np.sqrt(3) / 4:
        return 'butterfly'
    elif y + np.sqrt(3) * x <= np.sqrt(3) / 2:
        return 'unicorn'
    elif np.sqrt(3) * x - y >= np.sqrt(3) / 2:
        return 'bookworm'
    else:
        return 'cat?'


def get_personas(event_sessions=None, visitors=None, output_file=None):
    """
    Group session use cases into personas

    :param df event_sessions: events data aggregated by sessions with defined use_case
    :param df visitors: visitors data
    :param str output_file: optional, outputs persona data to csv to plot triangle
    """
    personas = event_sessions.groupby('dg_id').use_case.apply(list).reset_index()
    personas = pd.merge(personas, visitors[['dg_id', 'primary_specialty']], on='dg_id')
    personas['total_sessions'] = personas.use_case.apply(lambda x: len(x))
    personas['use_case_counts'] = personas.use_case.apply(lambda x: Counter(x))

    # parse out use case counts
    personas['pharma'] = personas.use_case_counts.apply(lambda x: x['pharma_device'])
    personas['publications'] = personas.use_case_counts.apply(lambda x: x['publications_ed_tools'])
    personas['social'] = personas.use_case_counts.apply(lambda x: x['professional_social_media'])
    personas['total_3d'] = personas.apply(lambda x: x.pharma + x.publications + x.social, axis=1)

    # exclude folks with only 'other' category (will address eventually)
    personas = personas[personas.total_3d > 0]

    # normalize per user
    personas['pharma_pct'] = personas.apply(lambda x: x.pharma / x.total_3d, axis=1)
    personas['pubs_pct'] = personas.apply(lambda x: x.publications / x.total_3d, axis=1)
    personas['social_pct'] = personas.apply(lambda x: x.social / x.total_3d, axis=1)

    # get coordinates to define persona
    personas['ternary_coordinates'] = personas.apply(lambda x: to_cartesian(x.pharma_pct, x.pubs_pct, x.social_pct), axis=1)
    personas['persona'] = personas.ternary_coordinates.apply(lambda x: coordinates_to_persona(x[0], x[1]))
    personas['ternary_x'] = personas.ternary_coordinates.apply(lambda x: x[0])
    personas['ternary_y'] = personas.ternary_coordinates.apply(lambda x: x[1])

    if output_file:
        personas.to_csv(output_file, index=False)

    return personas
