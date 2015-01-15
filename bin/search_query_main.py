
# coding: utf-8

from rocketads.oauth2.credentials import get_default_production_client
from rocketads.adwords.reports import get_report_for_accounts
from rocketads.adwords.hierarchy import get_mcc_hierarchy_raw
from rocketads.utils.misc import get_date_in_the_past, change_logger_level, remove_logger_handlers
from rocketads.utils.misc import is_valid_broad_modified
from rocketads.utils.misc import make_a_logger
import sys
from rocketads.utils.string_processing import TextNormalizer
import pandas as pd
from forex import forex
from datetime import date
from rocketads.adwords.templates import ConvDict
from rocketads.utils.mailme import mailto
from config import OPTIONS

_logger = make_a_logger('sq_negative_report')


def get_search_query_report(mode,TOP_MCC, country, start_day=2,end_day=14+2):
    #The first step is to create a client for the top level mcc

    #import ipdb; ipdb.set_trace()


    client = get_default_production_client(TOP_MCC)

    #This datatructure holds all sub mccs for the country
    graph = get_mcc_hierarchy_raw(client, return_top_level=False)



    all_accounts = {customer.name: customer.customerId for customer in graph.entries}

    all_accounts={name:val for name,val in all_accounts.items() if 'SEM' in name and ('LVN' in name or 'LPH' in name or
                                                                                      'LID' in name or 'LTH' in name or
                                                                                      'LSG' in name or 'LMY' in name)}

    if mode!='PRODUCTION':
        all_accounts = dict(all_accounts.items()[:3])

    # FIXME : Filter by country all accounts

    for acc in all_accounts:
        _logger.debug(u'account {}'.format(acc))

    #We look for all history up to now
    end_time = get_date_in_the_past(start_day)
    start_time = get_date_in_the_past(end_day)

    query = """
            SELECT AdGroupId, AdGroupName,CampaignId,CampaignName, CampaignStatus, Clicks, Impressions,
            KeywordTextMatchingQuery, Query, Cost, ConversionValue, MatchType, Conversions
            FROM SEARCH_QUERY_PERFORMANCE_REPORT
            WHERE CampaignStatus=ENABLED AND AdGroupStatus=ENABLED
            DURING {}, {}
            """.format(start_time, end_time)



    #The query will be executed in parallel for all sub mccs in uganda
    df = get_report_for_accounts(query, all_accounts.values(), nthreads=20)
    df['start_date']=start_time
    df['end_date']=end_time
    #The return value is a dataframe which concatenates the results of the queries in each sub account
    return df





def label_bad_search_queries(df,options):

    Norm = TextNormalizer()

    df['search_term_norm'] = df.search_term.apply(Norm.normalize)

    # Aggregate search terms by cost
    agg_data_frame = []
    for key, group in df.groupby(['search_term_norm']):
        tot_cost=group.cost.sum()
        tot_impressions=group.impressions.sum()
        tot_conv_clicks = group.converted_clicks.sum()
        tot_value=group.total_conv_value.sum()
        d={'search_term_norm':key, 'tot_impressions':tot_impressions,
           'tot_cost': tot_cost,
           'tot_conv_clicks':tot_conv_clicks,
           'tot_value':tot_value }
        agg_data_frame.append(d)
    
    
    agg_data_frame=pd.DataFrame(agg_data_frame)
    agg_data_frame[agg_data_frame.tot_conv_clicks > 0]


    # merge with original data
    df2 = pd.merge(df,agg_data_frame,how='left',on=['search_term_norm'])

    fx = forex()
    to_currency = options.currency_code_cost
    from_currency = options.currency_code_values
    dt = str(date.today())


    m_price = fx.get_currency(dt,from_currency,to_currency)



    df2['tot_cost_conv'] = df2['tot_cost'] * m_price





    df2['tot_cir'] = df2['tot_cost_conv']/df2['tot_value']



    df2[(df2.tot_cir > 0) & (df2.tot_cir != pd.np.inf)]



    camp_multiplier=options.camp_multiplier
    ag_multiplier= options.ag_multiplier
    cir_treshold=options.cir_threshold
    cost_treshold=options.cost_threshold

    def is_bad(row):
        if (row['tot_value']!= 0) and row['tot_cir'] >= camp_multiplier* cir_treshold:
            return 'CAMP_NEGATIVE--'
        elif (row['tot_value']!= 0) and row['tot_cir'] >= ag_multiplier* cir_treshold:
            return 'AG_NEGATIVE'
        elif (row['tot_value']== 0) and row['tot_cost'] >= camp_multiplier* cost_treshold:
            return 'CAMP_NEGATIVE'
        elif (row['tot_value']== 0) and row['tot_cost'] >= ag_multiplier* cost_treshold:
            return 'AG_NEGATIVE'
        else:
            return 'FINE'

    df2['is_bad_search_term']=df2.apply(is_bad,axis=1)

    df2=df2[df2.is_bad_search_term!='FINE']

    return df2



def make_options_for_country(options_df,country):
    for el in options_df.to_dict('records'):
        if el['country'] == country:
            return ConvDict(el)



def main(mode,country,emails, start_day=2,end_day=16):

    options = make_options_for_country(OPTIONS,country)
    df = get_search_query_report(mode,options.top_mcc, country, start_day=2,end_day=14+2)

    df=label_bad_search_queries(df,options)

    ofilename= '{}_sq_{}.csv'.format(country,get_date_in_the_past(0))
    df.to_csv(ofilename,encoding='utf8')

    for email in emails:
        mailto(email,'negative_search_queries_{}'.format(country),attach=ofilename)


if __name__ == "__main__":

    import sys
    mode = sys.argv[1]
    country=sys.argv[2]
    emails = sys.argv[3:]

    main(mode,country,emails)










