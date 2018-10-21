#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Functions for cleaning and transforming pandas dataframes
'''

from copy import deepcopy

import pandas as pd
from scipy import stats


#####################################
# Helper Functions                  #
#####################################
def _notGrouped(df):
    ''' Produces a column indicating whether a code is finished being grouped '''
    out = (
        (df
         .groupby('id1')
         .groupID
         .transform('nunique') > 1.0
         )
        .groupby(df['groupID'])
        .transform('max')
    )
    return out

def _two_columnize(df, cols, within):
    ''' creates 2 groups out of the grouping columns and within vars '''
    dfu = pd.DataFrame()
    col_lists = []
    maps = []

    for i in range(2):
        col_lists.append(cols[i] + within)
        dfu[f'id{i}'] = df.groupby(col_lists[i]).ngroup()
        maps.append(df[col_lists[i]].assign(**{f'id{i}': dfu[f'id{i}']}).drop_duplicates())

    dfu.drop_duplicates(inplace=True)
    dfu.reset_index(drop=True, inplace=True)

    return dfu, maps[0], maps[1]


def _pntCnt(itr, df):
    ''' Prints info on the grouping process '''
    ug = df['notDone'].sum()
    ug_frac = df['notDone'].sum() / df.shape[0]
    
    print(
            'iteration: {}'.format(itr),
            'ungrouped rows: {}'.format(ug),
            'ungrouped fraction: {:.1%}'.format(ug_frac),
            '',
        sep='\n')
    return (itr + 1)


def _chklst(item):
    if isinstance(item, str):
        item = [item]
    else:
        assert isinstance(item, list)
    return item

#####################################
# Main Functions                    #
#####################################

def mode_val(x):
    ''' mode function for pandas agg: gets value '''
    return stats.mode(x, nan_policy='omit')[0][0]

def mode_cnt(x):
    ''' mode function for pandas agg: gets count '''
    return stats.mode(x, nan_policy='omit')[1][0]

def convert_dates(df, cols=None, dformat=None, inplace=True):
    ''' converts string dates to datetime objects. Default inplace.'''
    if cols is None:
        cols = [i for i in df.select_dtypes(['object']).columns if (('date' in i) | ('time' in i))]

    if not inplace:
        df = deepcopy(df[cols])

    fails = [] 
    for i in cols:
        try:
            df[i] = pd.to_datetime(df[i], infer_datetime_format=True, format=dformat)
        except ValueError:
            fails.append(i)

    if len(fails) >= 1:
        print(f'Could not convert columns: {",".join(fails)}')

    if not inplace:
        return cols, df


def groupConcord(df, cols=None, within=[], pntCnt=False, dropids=False):
    '''
    
    Given two columns of many-to-many relationships, creates a m:1 relationship from each column to 
    a new, grouped code. This does not play well with nans at the moment. Ensure there are no nans 
    in your set.
    
    Parameters
    ----------
    df : pandas DataFrame object
        The dataframe containing the columns you want mapped together,
        each row representing a mapping from one variable to the other.
    cols : list, length 2, default None
        List of column names for grouping -- must be of length 2. Each element can be string or 
        array-like. If one of the elements is array-like, it implies that the grouping should be 
        done using the set of unique combinations of the nested-elements.
        If blank, assumes the first 2 columns of the DataFrame.
    within : string or list, default None
        List of column names to group within. Equivalent to adding the 'within' variables to both
        sets of columns. 
        e.g. cols=['a', 'b'], within='year' is equivalent to cols = [['a', 'year'], ['b', 'year']]
    pntCnt : Boolean, default False
        If True, prints the iteration of the groupby function and the number of observations still 
        ungrouped and that as a fraction of the total. Used for performance testing.
        
        
    Examples
    --------
    >>> df = pd.DataFrame([[1,2,8],[1,3,7],[1,3,9],[1,4,5],[2,12,0],[2,14,0],[3,3,0],[4,20,0]],columns= ['a','b','c'])
    >>> groupConcord(df, cols = ['a','b'], dropids=True)
    
    Out:
           groupID  a   b
        0        0  1   2
        1        0  1   3
        2        0  3   3
        3        0  1   4
        4        1  2  12
        5        1  2  14
        6        3  4  20
    
    Returns
    -------
    pandas DataFrame object
    
    '''
    #Prepare the set and check inputs
    if cols is None:
        cols = list(df.columns[0:2])

    if isinstance(within, str):
        within = [within]
        
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Only takes a pandas dataframe")
        
    if len(cols) > 2:
        raise ValueError("Can only group 2 column groups")

    cols = [_chklst(cols[0]), _chklst(cols[1])]
    within = _chklst(within)

    dfu, g0_map, g1_map = _two_columnize(df, cols, within)

    #get unique mappings and gen the initial groupings
    dfu['groupID'] = dfu['id0']
    
    #indicator for if the group is finished
    dfu['notDone'] = _notGrouped(dfu)
    
    if pntCnt:
        itr = _pntCnt(1, dfu)
    
    # Iterate over the grouping process for the subset of non-grouped
    # Could be more efficient by only doing the ones not finished
    while dfu.notDone.any():
        dfr = dfu[dfu['notDone']].copy()
        
        dfr['groupID'] = dfr.groupby('id1').groupID.transform('min')
        dfr['groupID'] = dfr.groupby('id0').groupID.transform('min')
        
        dfu['notDone'].update(_notGrouped(dfr))
        dfu['groupID'].update(dfr['groupID'])
        
        if pntCnt:
            itr = _pntCnt(itr, dfu)

    out = (
        dfu[['groupID', 'id0', 'id1']]
        .merge(g0_map, on='id0', validate='m:1')
        .merge(g1_map, on=(['id1'] + within), validate='m:1')
    )

    if dropids:
        out.drop(['id0', 'id1'], axis=1, inplace=True)
    
    return out





