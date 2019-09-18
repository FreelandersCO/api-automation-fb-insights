from datetime import datetime, timedelta
import pandas as pd
import calendar , facebook, json, sys, requests, argparse, time, threading, os
 
#Data Base Operation
from datab import DatabaseOperation


class PageApp(object):
    def __init__(self, version, page_data):
        super(PageApp).__init__()
        self.database = DatabaseOperation()
        page_token = page_data.token
        self.page_id = page_data.id_page
        self.graph = facebook.GraphAPI(access_token=page_token, version=version)
        months = self.define_months()
        current_year = ''
        for month_thread in months:
            
            hilo = threading.Thread(target=self.write_daily_page_insights_into_database,
                            kwargs={'year':month_thread['year'], 
                                    'month':month_thread['month']})

            if(current_year != '' and current_year != month_thread['year']) :
                print('Sleep Page Data By Year (15 sec)')
                time.sleep(15)
            
            current_year = month_thread['year']
            hilo.start()

    def define_months(self):
        page_insight = self.database.select('page','id_page',self.page_id)
        month_list = []
        currentDT = datetime.now()
        if len(page_insight) == 0:
            for year in range(2017, currentDT.year + 1):
                if(year==currentDT.year):
                    month_range = currentDT.month + 1
                else:
                    month_range = 13

                for i in range(1, month_range):
                    month= {
                        'year': year,
                        'month': i
                    }
                    month_list.append(month)
                    del month
        else:
            current_month = currentDT.month
            current_year = currentDT.year
            past_month = current_month - 1
            past_year = current_year
            
            if(current_month==1):
                past_month = 12
                past_year = past_year -1
            
            month= {
                'year': current_year,
                'month': current_month
            }
            month_list.append(month)
            
            month= {
                'year': past_year,
                'month': past_month
            }
            month_list.append(month)

            del month

        return month_list

    def write_daily_page_insights_into_database(self, **task_data):
        year = task_data['year']
        month = task_data['month']
        year_str = str(year)
        month_str = str(month)
        page_id = self.page_id
        file_name = 'temp/'+page_id+'_Page_insights_'+year_str+'_'+month_str+'.json'
        # Get Data
        df_demographics = self.read_daily_demographics_insights_into_df(year, month)
        df_impressions = self.read_daily_impressions_insights_into_df(year, month)
        df_engagement = self.read_daily_engagement_insights_into_df(year, month)
        df_reactions = self.read_daily_reactions_insights_into_df(year, month)
        df_page = df_demographics.join(df_impressions).join(df_engagement).join(df_reactions)
        df_page.to_json(f'{file_name}',orient='split')

        # Read json
        with open(file_name, 'r') as f:
            data = json.load(f)

        column_data = []
        for column in data['columns']:
            column_data.append(column[0])

        for data_line in data['data']:
            data_to_database = {}
            row = 0

            data_to_database['id_page'] = page_id
            data_to_database['year_data'] = year_str
            data_to_database['month_data'] = month_str
            
            for data_value in data_line:
                name = column_data[row].lower()
                data_to_database[name] = data_value
                row = row + 1
            
            date_object = datetime.strptime(data_to_database['date_data'], '%Y-%m-%d') - timedelta(days=1)
            data_to_database['date_data'] = date_object
            self.database.insert('page', data_to_database)
            del data_to_database
            del row
            if os.path.exists(file_name):
                os.remove(file_name)
        
    def page_daily_insights_for_month(self, metric, year, month, period='day'):
        next_year = year
        next_month = month + 1
        if(month == 12):
            next_year = year + 1
            next_month = 1
        return self.graph.get_connections(id=self.page_id,
                                          connection_name='insights',
                                          metric=metric,
                                          period=period,
                                          since=datetime(year, month, 1, 0, 0, 0),
                                          until=datetime(next_year, next_month, 1, 23, 59, 59),
                                          show_description_from_api_doc=True)

    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------- PAGE INSIGHTS --------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # -------------------------------------------------- DEMOGRAPHICS --------------------------------------------------
    def read_daily_demographics_insights_into_df(self, year, month):
        df_page = pd.DataFrame(columns=pd.MultiIndex(levels=[[], []], codes=[[], []]))
        
        # The total number of people who have liked your Page. (Unique Users). Lifetime
        fans = self.page_daily_insights_for_month('page_fans', year, month)
        end_time, value = list(), list()
        
        if len(fans['data']) > 0 :
            for item in fans['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)

            # df_page['date_data'] = pd.Series(data=end_time, index=end_time, name='date_data')
            df_page['fans'] = pd.Series(data=value, index=end_time, name='value')

        # The number of people who liked your Page, broken down by the most common places where people can like
        # your Page.(Unique Users). Daily
        fans_by_like_source_unique = self.page_daily_insights_for_month('page_fans_by_like_source_unique', year, month)
        end_time, news_feed, other = list(), list(), list()
        ads, page_suggestions, restored_likes, search, your_page = list(), list(), list(), list(), list()
        if len(fans_by_like_source_unique['data']) > 0 :
            for item in fans_by_like_source_unique['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    ads.append(item['value']['Ads'])
                except:
                    ads.append(0)
                try:
                    news_feed.append(item['value']['News Feed'])
                except:
                    news_feed.append(0)
                try:
                    other.append(item['value']['Other'])
                except:
                    other.append(0)
                try:
                    page_suggestions.append(item['value']['Page Suggestions'])
                except:
                    page_suggestions.append(0)
                try:
                    restored_likes.append(item['value']['Restored Likes from Reactivated Accounts'])
                except:
                    restored_likes.append(0)
                try:
                    search.append(item['value']['Search'])
                except:
                    search.append(0)
                try:
                    your_page.append(item['value']['Your Page'])
                except:
                    your_page.append(0)

            df_page['date_data'] = pd.Series(data=end_time, index=end_time, name='date_data')
            df_page['fans_by_like_source_unique_ads'] = pd.Series(data=ads, index=end_time, name='ads')
            df_page['fans_by_like_source_unique_news_feed'] = pd.Series(data=news_feed, index=end_time, name='news_feed')
            df_page['fans_by_like_source_unique_other'] = pd.Series(data=other, index=end_time, name='other')
            df_page['fans_by_like_source_unique_page_suggestions'] = pd.Series(data=page_suggestions, index=end_time, name='page_suggestions')
            df_page['fans_by_like_source_unique_restored_likes'] = pd.Series(data=restored_likes, index=end_time, name='restored_likes')
            df_page['fans_by_like_source_unique_search'] = pd.Series(data=search, index=end_time, name='search')
            df_page['fans_by_like_source_unique_your_page'] = pd.Series(data=your_page, index=end_time, name='your_page')

        # This is a breakdown of the number of Page likes from the most common places where people can like your Page.
        # (Total Count). Daily
        fans_by_like_source = self.page_daily_insights_for_month('page_fans_by_like_source', year, month)
        end_time, news_feed, other = list(), list(), list()
        ads, page_suggestions, restored_likes, search, your_page = list(), list(), list(), list(), list()
       
        if len(fans_by_like_source['data']) > 0 :
            for item in fans_by_like_source['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    ads.append(item['value']['Ads'])
                except:
                    ads.append(0)
                try:
                    news_feed.append(item['value']['News Feed'])
                except:
                    news_feed.append(0)
                try:
                    other.append(item['value']['Other'])
                except:
                    other.append(0)
                try:
                    page_suggestions.append(item['value']['Page Suggestions'])
                except:
                    page_suggestions.append(0)
                try:
                    restored_likes.append(item['value']['Restored Likes from Reactivated Accounts'])
                except:
                    restored_likes.append(0)
                try:
                    search.append(item['value']['Search'])
                except:
                    search.append(0)
                try:
                    your_page.append(item['value']['Your Page'])
                except:
                    your_page.append(0)

            df_page['fans_by_like_source_ads'] = pd.Series(data=ads, index=end_time, name='ads')
            df_page['fans_by_like_source_news_feed'] = pd.Series(data=news_feed, index=end_time, name='news_feed')
            df_page['fans_by_like_source_other'] = pd.Series(data=other, index=end_time, name='other')
            df_page['fans_by_like_source_page_suggestions'] = pd.Series(data=page_suggestions, index=end_time, name='page_suggestions')
            df_page['fans_by_like_source_restored_likes'] = pd.Series(data=restored_likes, index=end_time,name='restored_likes')
            df_page['fans_by_like_source_search'] = pd.Series(data=search, index=end_time, name='search')
            df_page['fans_by_like_source_your_page'] = pd.Series(data=your_page, index=end_time, name='your_page')
        
        # The number of people, aggregated per country, that like your Page. Only the 45 countries with the most people that like your Page are included.
        page_fans_country = self.page_daily_insights_for_month('page_fans_country', year, month)
        end_time , country_data = list(), list()
        
        if len(page_fans_country['data']) > 0 :
            for item in page_fans_country['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    country_data.append(item['value'])
                except:
                    country_data.append('ND')
            
            df_page['fans_country'] = pd.Series(data=country_data, index=end_time, name='page_fans_country')

        #Aggregated Facebook location data, sorted by city, about the people who like your Page.
        page_fans_city = self.page_daily_insights_for_month('page_fans_city', year, month)
        end_time , city_data = list(), list()

        if len(page_fans_city['data']) > 0 :
            for item in page_fans_city['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    city_data.append(item['value'])
                except:
                    city_data.append('ND')
            
            df_page['fans_city'] = pd.Series(data=city_data, index=end_time, name='page_fans_city')

        return df_page

    # --------------------------------------------------- IMPRESSIONS --------------------------------------------------
    def read_daily_impressions_insights_into_df(self, year, month):
        df_page = pd.DataFrame(columns=pd.MultiIndex(levels=[[], []], codes=[[], []]))

        # The number of people who had any content from your Page or about your Page enter their screen. This includes
        # posts, check-ins, ads, social information from people who interact with your Page and more. (Unique Users).
        # Daily
        unique = self.page_daily_insights_for_month('page_impressions_unique', year, month)
        end_time, value = list(), list()
        if len(unique['data']) > 0 :
            for item in unique['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)

            df_page['impressions_unique'] = pd.Series(data=value, index=end_time, name='value')
        
        # The number of times any content from your Page or about your Page entered a person's screen.
        # This includes posts, check-ins, ads, social information from people who interact with your Page and more.
        # (Total Count). Daily
        impressions = self.page_daily_insights_for_month('page_impressions', year, month)
        end_time, value = list(), list()
        if len(impressions['data']) > 0 :
            for item in impressions['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)

            df_page['impressions'] = pd.Series(data=value, index=end_time, name='value')

        # Total number of people who saw a story about your Page by story type. (Unique Users). Daily
        by_story_unique = self.page_daily_insights_for_month('page_impressions_by_story_type_unique', year, month)
        end_time, mention, other, fan, page_post, checkin = list(), list(), list(), list(), list(), list()

        if len(by_story_unique['data']) > 0 :

            for item in by_story_unique['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    mention.append(item['value']['mention'])
                except:
                    mention.append(0)
                try:
                    other.append(item['value']['other'])
                except:
                    other.append(0)
                try:
                    fan.append(item['value']['fan'])
                except:
                    fan.append(0)
                try:
                    page_post.append(item['value']['page post'])
                except:
                    page_post.append(0)
                try:
                    checkin.append(item['value']['checkin'])
                except:
                    checkin.append(0)

            df_page['impressions_by_story_unique_mention'] = pd.Series(data=mention, index=end_time, name='mention')
            df_page['impressions_by_story_unique_other'] = pd.Series(data=other, index=end_time, name='other')
            df_page['impressions_by_story_unique_fan'] = pd.Series(data=fan, index=end_time, name='fan')
            df_page['impressions_by_story_unique_page_post'] = pd.Series(data=page_post, index=end_time,name='page_post')
            df_page['impressions_by_story_unique_checkin'] = pd.Series(data=checkin, index=end_time, name='checkin')

        #  Total impressions of stories published by a friend about your Page by story type. (Total Count). Daily
        by_story = self.page_daily_insights_for_month('page_impressions_by_story_type', year, month)
        end_time, mention, other, fan, page_post, checkin = list(), list(), list(), list(), list(), list()
        
        if len(by_story['data']) > 0 :
            for item in by_story['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    mention.append(item['value']['mention'])
                except:
                    mention.append(0)
                try:
                    other.append(item['value']['other'])
                except:
                    other.append(0)
                try:
                    fan.append(item['value']['fan'])
                except:
                    fan.append(0)
                try:
                    page_post.append(item['value']['page post'])
                except:
                    page_post.append(0)
                try:
                    checkin.append(item['value']['checkin'])
                except:
                    checkin.append(0)

            df_page['impressions_by_story_mention'] = pd.Series(data=mention, index=end_time, name='mention')
            df_page['impressions_by_story_other'] = pd.Series(data=other, index=end_time, name='other')
            df_page['impressions_by_story_fan'] = pd.Series(data=fan, index=end_time, name='fan')
            df_page['impressions_by_story_page_post'] = pd.Series(data=page_post, index=end_time, name='page_post')
            df_page['impressions_by_story_checkin'] = pd.Series(data=checkin, index=end_time, name='checkin')

        # Total Page Reach by user country. (Unique Users). Daily
        by_country = self.page_daily_insights_for_month('page_impressions_by_country_unique', year, month)
        end_time, impressions_by_country = list(), list()

        if len(by_country['data']) > 0 :

            for item in by_country['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                impressions_by_country.append(item['value'])

            df_page['impressions_by_country'] = pd.Series(data=impressions_by_country, index=end_time, name='impressions_by_country')
        
        # Total Page Reach by city country. (Unique Users). Daily
        by_impressions_by_city, end_time, impressions_by_city = list(), list(), list()
        by_impressions_by_city = self.page_daily_insights_for_month('page_impressions_by_city_unique', year, month)
        
        if len(by_impressions_by_city['data']) > 0 :
            for item in by_impressions_by_city['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                impressions_by_city.append(item['value'])

            df_page['impressions_by_city'] = pd.Series(data=impressions_by_city, index=end_time, name='by_impressions_by_city')

        # Total Page Reach by age and gender. (Unique Users). Daily
        by_age_gender_unique = self.page_daily_insights_for_month('page_fans_gender_age', year, month)
        end_time = list()
        f_13_17, f_18_24, f_25_34, f_35_44, f_45_54, f_55_64 = list(), list(), list(), list(), list(), list()
        f_65_plus = list()
        m_13_17, m_18_24, m_25_34, m_35_44, m_45_54, m_55_64 = list(), list(), list(), list(), list(), list()
        m_65_plus = list()
        u_13_17, u_18_24, u_25_34, u_35_44, u_45_54, u_55_64 = list(), list(), list(), list(), list(), list()
        u_65_plus = list()

        if len(by_age_gender_unique['data']) > 0 :
            for item in by_age_gender_unique['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    f_13_17.append(item['value']['F.13-17'])
                except:
                    f_13_17.append(0)

                try:
                    f_18_24.append(item['value']['F.18-24'])
                except:
                    f_18_24.append(0)

                try:
                    f_25_34.append(item['value']['F.25-34'])
                except:
                    f_25_34.append(0)

                try:
                    f_35_44.append(item['value']['F.35-44'])
                except:
                    f_35_44.append(0)

                try:
                    f_45_54.append(item['value']['F.45-54'])
                except:
                    f_45_54.append(0)

                try:
                    f_55_64.append(item['value']['F.55-64'])
                except:
                    f_55_64.append(0)

                try:
                    f_65_plus.append(item['value']['F.65+'])
                except:
                    f_65_plus.append(0)

                try:
                    m_13_17.append(item['value']['M.13-17'])
                except:
                    m_13_17.append(0)

                try:
                    m_18_24.append(item['value']['M.18-24'])
                except:
                    m_18_24.append(0)

                try:
                    m_25_34.append(item['value']['M.25-34'])
                except:
                    m_25_34.append(0)

                try:
                    m_35_44.append(item['value']['M.35-44'])
                except:
                    m_35_44.append(0)

                try:
                    m_45_54.append(item['value']['M.45-54'])
                except:
                    m_45_54.append(0)

                try:
                    m_55_64.append(item['value']['M.55-64'])
                except:
                    m_55_64.append(0)

                try:
                    m_65_plus.append(item['value']['M.65+'])
                except:
                    m_65_plus.append(0)

                try:
                    u_13_17.append(item['value']['U.13-17'])
                except:
                    u_13_17.append(0)

                try:
                    u_18_24.append(item['value']['U.18-24'])
                except:
                    u_18_24.append(0)

                try:
                    u_25_34.append(item['value']['U.25-34'])
                except:
                    u_25_34.append(0)

                try:
                    u_35_44.append(item['value']['U.35-44'])
                except:
                    u_35_44.append(0)

                try:
                    u_45_54.append(item['value']['U.45-54'])
                except:
                    u_45_54.append(0)

                try:
                    u_55_64.append(item['value']['U.55-64'])
                except:
                    u_55_64.append(0)

                try:
                    u_65_plus.append(item['value']['U.65+'])
                except:
                    u_65_plus.append(0)


            df_page['impressions_by_age_gender_unique_F_13_17'] = pd.Series(data=f_13_17, index=end_time, name='F_13_17')
            df_page['impressions_by_age_gender_unique_F_18_24'] = pd.Series(data=f_18_24, index=end_time, name='F_18_24')
            df_page['impressions_by_age_gender_unique_F_25_34'] = pd.Series(data=f_25_34, index=end_time, name='F_25_34')
            df_page['impressions_by_age_gender_unique_F_35_44'] = pd.Series(data=f_35_44, index=end_time, name='F_35_44')
            df_page['impressions_by_age_gender_unique_F_45_54'] = pd.Series(data=f_45_54, index=end_time, name='F_45_54')
            df_page['impressions_by_age_gender_unique_F_55_64'] = pd.Series(data=f_55_64, index=end_time, name='F_55_64')
            df_page['impressions_by_age_gender_unique_F_65'] = pd.Series(data=f_65_plus, index=end_time, name='F_65')
            df_page['impressions_by_age_gender_unique_M_13_17'] = pd.Series(data=m_13_17, index=end_time, name='M_13_17')
            df_page['impressions_by_age_gender_unique_M_18_24'] = pd.Series(data=m_18_24, index=end_time, name='M_18_24')
            df_page['impressions_by_age_gender_unique_M_25_34'] = pd.Series(data=m_25_34, index=end_time, name='M_25_34')
            df_page['impressions_by_age_gender_unique_M_35_44'] = pd.Series(data=m_35_44, index=end_time, name='M_35_44')
            df_page['impressions_by_age_gender_unique_M_45_54'] = pd.Series(data=m_45_54, index=end_time, name='M_45_54')
            df_page['impressions_by_age_gender_unique_M_55_64'] = pd.Series(data=m_55_64, index=end_time, name='M_55_64')
            df_page['impressions_by_age_gender_unique_M_65'] = pd.Series(data=m_65_plus, index=end_time, name='M_65')
            df_page['impressions_by_age_gender_unique_U_13_17'] = pd.Series(data=u_13_17, index=end_time, name='U_13_17')
            df_page['impressions_by_age_gender_unique_U_18_24'] = pd.Series(data=u_18_24, index=end_time, name='U_18_24')
            df_page['impressions_by_age_gender_unique_U_25_34'] = pd.Series(data=u_25_34, index=end_time, name='U_25_34')
            df_page['impressions_by_age_gender_unique_U_35_44'] = pd.Series(data=u_35_44, index=end_time, name='U_35_44')
            df_page['impressions_by_age_gender_unique_U_45_54'] = pd.Series(data=u_45_54, index=end_time, name='U_45_54')
            df_page['impressions_by_age_gender_unique_U_55_64'] = pd.Series(data=u_55_64, index=end_time, name='U_55_64')
            df_page['impressions_by_age_gender_unique_U_65'] = pd.Series(data=u_65_plus, index=end_time, name='U_65')
        
        #page_impressions_unique_28_days
        page_impressions_days_28 = self.page_daily_insights_for_month('page_impressions', year, month, 'days_28')
        end_time, value = list(), list()

        if len(page_impressions_days_28['data']) > 0 :

            for item in page_impressions_days_28['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)
            
            df_page['impressions_28_days'] = pd.Series(data=value, index=end_time, name='value')

        #page_impressions_unique_28_days
        page_impressions_unique_28_days = self.page_daily_insights_for_month('page_impressions_unique', year, month, 'days_28')
        end_time, value = list(), list()

        if len(page_impressions_unique_28_days['data']) > 0 :

            for item in page_impressions_unique_28_days['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)
            
            df_page['impressions_unique_28_days'] = pd.Series(data=value, index=end_time, name='value')

        #page_impressions_organic_unique

        page_impressions_organic_unique = self.page_daily_insights_for_month('page_impressions_organic_unique', year, month)
        end_time, value = list(), list()
        
        if len(page_impressions_organic_unique['data']) > 0 :

            for item in page_impressions_organic_unique['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)
            
            df_page['impressions_organic_unique'] = pd.Series(data=value, index=end_time, name='value')

        page_impressions_organic_unique_28_days = self.page_daily_insights_for_month('page_impressions_organic_unique', year, month, 'days_28')
        end_time, value = list(), list()
        
        if len(page_impressions_organic_unique_28_days['data']) > 0 :

            for item in page_impressions_organic_unique_28_days['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)
            
            df_page['impressions_organic_unique_28_days'] = pd.Series(data=value, index=end_time, name='value')
        
        page_impressions_paid_unique = self.page_daily_insights_for_month('page_impressions_paid_unique', year, month)
        end_time, value = list(), list()
            
        if len(page_impressions_paid_unique['data']) > 0 :

            for item in page_impressions_paid_unique['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)
            
            df_page['impressions_paid_unique'] = pd.Series(data=value, index=end_time, name='value')

        page_impressions_paid_unique_28_days = self.page_daily_insights_for_month('page_impressions_paid_unique', year, month, 'days_28')
        end_time, value = list(), list()
            
        if len(page_impressions_paid_unique_28_days['data']) > 0 :

            for item in page_impressions_paid_unique_28_days['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)
            
            df_page['impressions_paid_unique_28_days'] = pd.Series(data=value, index=end_time, name='value')
        
        page_impressions_organic = self.page_daily_insights_for_month('page_impressions_organic', year, month)
        end_time, value = list(), list()
            
        if len(page_impressions_organic['data']) > 0 :

            for item in page_impressions_organic['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)
            
            df_page['impressions_organic'] = pd.Series(data=value, index=end_time, name='value')

        page_impressions_organic_28_days = self.page_daily_insights_for_month('page_impressions_organic', year, month, 'days_28')
        end_time, value = list(), list()
            
        if len(page_impressions_organic_28_days['data']) > 0 :

            for item in page_impressions_organic_28_days['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)
            
            df_page['impressions_organic_28_days'] = pd.Series(data=value, index=end_time, name='value')

        page_impressions_paid = self.page_daily_insights_for_month('page_impressions_paid', year, month)
        end_time, value = list(), list()
            
        if len(page_impressions_paid['data']) > 0 :

            for item in page_impressions_paid['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)
            
            df_page['impressions_paid'] = pd.Series(data=value, index=end_time, name='value')
        
        page_impressions_paid_28_days = self.page_daily_insights_for_month('page_impressions_paid', year, month, 'days_28')
        end_time, value = list(), list()
            
        if len(page_impressions_paid_28_days['data']) > 0 :

            for item in page_impressions_paid_28_days['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)
            
            df_page['impressions_paid_28_days'] = pd.Series(data=value, index=end_time, name='value')
        
        page_impressions_viral = self.page_daily_insights_for_month('page_impressions_viral', year, month)
        end_time, value = list(), list()
            
        if len(page_impressions_viral['data']) > 0 :

            for item in page_impressions_viral['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)
            
            df_page['impressions_viral'] = pd.Series(data=value, index=end_time, name='value')
        
        page_impressions_viral_28_days = self.page_daily_insights_for_month('page_impressions_viral', year, month, 'days_28')
        end_time, value = list(), list()
            
        if len(page_impressions_viral_28_days['data']) > 0 :

            for item in page_impressions_viral_28_days['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)
            
            df_page['impressions_viral_28_days'] = pd.Series(data=value, index=end_time, name='value')

        page_impressions_viral_unique = self.page_daily_insights_for_month('page_impressions_viral_unique', year, month)
        end_time, value = list(), list()
            
        if len(page_impressions_viral_unique['data']) > 0 :

            for item in page_impressions_viral_unique['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)
            
            df_page['impressions_viral_unique'] = pd.Series(data=value, index=end_time, name='value')
        
        page_impressions_viral_unique_28_days = self.page_daily_insights_for_month('page_impressions_viral_unique', year, month, 'days_28')
        end_time, value = list(), list()
            
        if len(page_impressions_viral_unique_28_days['data']) > 0 :

            for item in page_impressions_viral_unique_28_days['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)
            
            df_page['impressions_viral_unique_28_days'] = pd.Series(data=value, index=end_time, name='value')

        return df_page

    # --------------------------------------------------- ENGAGEMENT ---------------------------------------------------
    def read_daily_engagement_insights_into_df(self, year, month):
        df_page = pd.DataFrame(columns=pd.MultiIndex(levels=[[], []], codes=[[], []]))

        # The number of people who engaged with your Page. Engagement includes any click or story created.
        # (Unique Users). Daily
        engaged_users = self.page_daily_insights_for_month('page_engaged_users', year, month)
        end_time, value = list(), list()

        if len(engaged_users['data']) > 0 :
            for item in engaged_users['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)

            df_page['engaged_users'] = pd.Series(data=value, index=end_time, name='value')

        # The number of of people who clicked on any of your content, by type. Stories that are created without clicking
        # on Page content (ex, liking the Page from timeline) are not included. (Unique Users). Daily
        by_consumption_type_unique = self.page_daily_insights_for_month('page_consumptions_by_consumption_type_unique', year, month)
        end_time, video_play, other_clicks, photo_view, link_clicks = list(), list(), list(), list(), list()
        
        if len(by_consumption_type_unique['data']) > 0 :
            for item in by_consumption_type_unique['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    video_play.append(item['value']['video play'])
                except:
                    video_play.append(0)
                try:
                    other_clicks.append(item['value']['other clicks'])
                except:
                    other_clicks.append(0)
                try:
                    photo_view.append(item['value']['photo view'])
                except:
                    photo_view.append(0)
                try:
                    link_clicks.append(item['value']['link clicks'])
                except:
                    link_clicks.append(0)

            df_page['consumptions_by_type_unique_video_play'] = pd.Series(data=video_play, index=end_time,name='video_play')
            df_page['consumptions_by_type_unique_other_clicks'] = pd.Series(data=other_clicks, index=end_time,name='other_clicks')
            df_page['consumptions_by_type_unique_photo_view'] = pd.Series(data=photo_view, index=end_time,name='photo_view')
            df_page['consumptions_by_type_unique_link_clicks'] = pd.Series(data=link_clicks, index=end_time,name='link_clicks')

        # The number of clicks on any of your content, by type. Stories generated without clicks on page content
        # (e.g., liking the page in Timeline) are not included. (Total Count). Daily
        by_consumption_type = self.page_daily_insights_for_month('page_consumptions_by_consumption_type', year, month)
        end_time, video_play, other_clicks, photo_view, link_clicks = list(), list(), list(), list(), list()
        
        if len(by_consumption_type['data']) > 0 :
            for item in by_consumption_type['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    video_play.append(item['value']['video play'])
                except:
                    video_play.append(0)
                try:
                    other_clicks.append(item['value']['other clicks'])
                except:
                    other_clicks.append(0)
                try:
                    photo_view.append(item['value']['photo view'])
                except:
                    photo_view.append(0)
                try:
                    link_clicks.append(item['value']['link clicks'])
                except:
                    link_clicks.append(0)

            df_page['consumptions_by_type_video_play'] = pd.Series(data=video_play, index=end_time, name='video_play')
            df_page['consumptions_by_type_other_clicks'] = pd.Series(data=other_clicks, index=end_time, name='other_clicks')
            df_page['consumptions_by_type_photo_view'] = pd.Series(data=photo_view, index=end_time, name='photo_view')
            df_page['consumptions_by_type_link_clicks'] = pd.Series(data=link_clicks, index=end_time, name='link_clicks')

        # Total check-ins at your Place (Unique Users). Daily
        places_checkin = self.page_daily_insights_for_month('page_places_checkin_total_unique', year, month)
        end_time, value = list(), list()

        if len(places_checkin['data']) > 0 :
            for item in places_checkin['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)

            df_page['places_checkin'] = pd.Series(data=value, index=end_time, name='value')

        # The number of people who have given negative feedback to your Page, by type. (Unique Users). Daily
        negative_feedback_by_type_unique = self.page_daily_insights_for_month('page_negative_feedback_by_type_unique', year, month)
        end_time, hide_all_clicks, hide_clicks, unlike_page_clicks = list(), list(), list(), list()
        report_spam_clicks = list()

        if len(negative_feedback_by_type_unique['data']) > 0 :
            for item in negative_feedback_by_type_unique['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    hide_all_clicks.append(item['value']['hide_all_clicks'])
                except:
                    hide_all_clicks.append(0)
                try:
                    hide_clicks.append(item['value']['hide_clicks'])
                except:
                    hide_clicks.append(0)
                try:
                    unlike_page_clicks.append(item['value']['unlike_page_clicks'])
                except:
                    unlike_page_clicks.append(0)
                try:
                    report_spam_clicks.append(item['value']['report_spam_clicks'])
                except:
                    report_spam_clicks.append(0)

            df_page['negative_feedback_by_type_unique_hide_all_clicks'] = pd.Series(data=hide_all_clicks, index=end_time,name='hide_all_clicks')
            df_page['negative_feedback_by_type_unique_hide_clicks'] = pd.Series(data=hide_clicks, index=end_time,name='hide_clicks')
            df_page['negative_feedback_by_type_unique_unlike_page_clicks'] = pd.Series(data=unlike_page_clicks,index=end_time,name='unlike_page_clicks')
            df_page['negative_feedback_by_type_unique_report_spam_clicks'] = pd.Series(data=report_spam_clicks,index=end_time,name='report_spam_clicks')

        # The number of times people have given negative feedback to your Page, by type. (Total Count). Daily
        negative_feedback_by_type = self.page_daily_insights_for_month('page_negative_feedback_by_type', year, month)
        end_time, hide_all_clicks, hide_clicks, unlike_page_clicks = list(), list(), list(), list()
        report_spam_clicks = list()
        if len(negative_feedback_by_type['data']) > 0 :
            for item in negative_feedback_by_type['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    hide_all_clicks.append(item['value']['hide_all_clicks'])
                except:
                    hide_all_clicks.append(0)
                try:
                    hide_clicks.append(item['value']['hide_clicks'])
                except:
                    hide_clicks.append(0)
                try:
                    unlike_page_clicks.append(item['value']['unlike_page_clicks'])
                except:
                    unlike_page_clicks.append(0)
                try:
                    report_spam_clicks.append(item['value']['report_spam_clicks'])
                except:
                    report_spam_clicks.append(0)

            df_page['negative_feedback_by_type_hide_all_clicks'] = pd.Series(data=hide_all_clicks, index=end_time, name='hide_all_clicks')
            df_page['negative_feedback_by_type_hide_clicks'] = pd.Series(data=hide_clicks, index=end_time, name='hide_clicks')
            df_page['negative_feedback_by_type_unlike_page_clicks'] = pd.Series(data=unlike_page_clicks, index=end_time, name='unlike_page_clicks')
            df_page['negative_feedback_by_type_report_spam_clicks'] = pd.Series(data=report_spam_clicks, index=end_time, name='report_spam_clicks')

        # The number of times people have given positive feedback to your Page, by type. (Unique Users). Daily
        positive_feedback_by_type_unique = self.page_daily_insights_for_month('page_positive_feedback_by_type_unique', year, month)
        end_time, link, like, comment, other = list(), list(), list(), list(), list()

        if len(positive_feedback_by_type_unique['data']) > 0 :
            for item in positive_feedback_by_type_unique['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    link.append(item['value']['link'])
                except:
                    link.append(0)
                try:
                    like.append(item['value']['like'])
                except:
                    like.append(0)
                try:
                    comment.append(item['value']['comment'])
                except:
                    comment.append(0)
                try:
                    other.append(item['value']['other'])
                except:
                    other.append(0)

            df_page['positive_feedback_by_type_unique_link'] = pd.Series(data=link, index=end_time, name='link')
            df_page['positive_feedback_by_type_unique_like'] = pd.Series(data=like, index=end_time, name='like')
            df_page['positive_feedback_by_type_unique_comment'] = pd.Series(data=comment, index=end_time, name='comment')
            df_page['positive_feedback_by_type_unique_other'] = pd.Series(data=other, index=end_time, name='other')

        # The number of times people have given positive feedback to your Page, by type. (Total Count). Daily
        positive_feedback_by_type = self.page_daily_insights_for_month('page_positive_feedback_by_type', year, month)
        end_time, link, like, comment, other = list(), list(), list(), list(), list()

        if len(positive_feedback_by_type['data']) > 0 :

            for item in positive_feedback_by_type['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    link.append(item['value']['link'])
                except:
                    link.append(0)
                try:
                    like.append(item['value']['like'])
                except:
                    like.append(0)
                try:
                    comment.append(item['value']['comment'])
                except:
                    comment.append(0)
                try:
                    other.append(item['value']['other'])
                except:
                    other.append(0)

            df_page['positive_feedback_by_type_link'] = pd.Series(data=link, index=end_time, name='link')
            df_page['positive_feedback_by_type_like'] = pd.Series(data=like, index=end_time, name='like')
            df_page['positive_feedback_by_type_comment'] = pd.Series(data=comment, index=end_time, name='comment')
            df_page['positive_feedback_by_type_other'] = pd.Series(data=other, index=end_time, name='other')

        return df_page

    # ---------------------------------------------------- REACTIONS ---------------------------------------------------
    def read_daily_reactions_insights_into_df(self, year, month):

        df_page = pd.DataFrame(columns=pd.MultiIndex(levels=[[], []], codes=[[], []]))

        # Total post like reactions of a page. Daily
        reactions_like_total = self.page_daily_insights_for_month('page_actions_post_reactions_like_total', year, month)
        end_time, value = list(), list()

        if len(reactions_like_total['data']) > 0 :
            for item in reactions_like_total['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)

            df_page['reactions_like'] = pd.Series(data=value, index=end_time, name='value')

        # Total post love reactions of a page. Daily
        reactions_love_total = self.page_daily_insights_for_month('page_actions_post_reactions_love_total', year, month)
        end_time, value = list(), list()

        if len(reactions_love_total['data']) > 0 :
            for item in reactions_love_total['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)

            df_page['reactions_love'] = pd.Series(data=value, index=end_time, name='value')

        # Total post wow reactions of a page. Daily
        reactions_wow_total = self.page_daily_insights_for_month('page_actions_post_reactions_wow_total', year, month)
        end_time, value = list(), list()

        if len(reactions_wow_total['data']) > 0 :
            for item in reactions_wow_total['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)

            df_page['reactions_wow'] = pd.Series(data=value, index=end_time, name='value')

        # Total post haha reactions of a page. Daily
        reactions_haha_total = self.page_daily_insights_for_month('page_actions_post_reactions_haha_total', year, month)
        end_time, value = list(), list()

        if len(reactions_haha_total['data']) > 0 :
            for item in reactions_haha_total['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)

            df_page['reactions_haha'] = pd.Series(data=value, index=end_time, name='value')

        # Total post sorry reactions of a page. Daily
        reactions_sorry_total = self.page_daily_insights_for_month('page_actions_post_reactions_sorry_total', year, month)
        end_time, value = list(), list()
        
        if len(reactions_sorry_total['data']) > 0 :
            for item in reactions_sorry_total['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)

            df_page['reactions_sorry'] = pd.Series(data=value, index=end_time, name='value')

        # Total post anger reactions of a page. Daily
        reactions_anger_total = self.page_daily_insights_for_month('page_actions_post_reactions_anger_total', year, month)
        end_time, value = list(), list()

        if len(reactions_anger_total['data']) > 0 :
            for item in reactions_anger_total['data'][0]['values']:
                end_time.append(item['end_time'][:10])
                try:
                    value.append(item['value'])
                except:
                    value.append(0)

            df_page['reactions_anger'] = pd.Series(data=value, index=end_time, name='value')

        return df_page
