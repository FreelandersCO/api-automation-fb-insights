# Import system necesary library
from datetime import datetime, timedelta
import pandas as pd
import calendar , facebook, json, sys, requests, argparse, time, threading
#Data Base Operation
from datab import DatabaseOperation

from multiprocessing.pool import ThreadPool


class PostApp(object):
    def __init__(self, version, page_data):
        super(PostApp).__init__()
        self.database = DatabaseOperation()

        self.graph = facebook.GraphAPI(access_token=page_data.token, version=version)

        months = self.define_months(page_data.id_page)
        current_month = ''
        for month_thread in months:
            hilo = threading.Thread(target=self.write_posts_insights_into_database,
                            kwargs={'year':month_thread['year'], 
                                    'month':month_thread['month'],
                                    'page_id': page_data.id_page})

            if(current_month != '' and current_month != month_thread['month']) :
                print('Sleep Post By Month (15 sec)')
                time.sleep(15)

            current_month = month_thread['month']
            hilo.start()

    def define_months(self, page_id):
        post_data = self.database.select('post','id_page',page_id)
        month_list = []
        currentDT = datetime.now()
        if len(post_data) == 0:
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

    def monthly_posts(self, page_id, year, month):
        last_day_in_month = calendar.monthrange(year, month)[1]
        return self.graph.get_all_connections(id=page_id,
                                              connection_name='posts',
                                              fields='type, name, message,created_time, object_id, is_hidden, comments.limit(500){message,id,created_time}',
                                              since=datetime(year, month, 1, 0, 0, 0),
                                              until=datetime(year, month, last_day_in_month, 23, 59, 59))


    def post_life_time_insights(self, post_id, metric):
        return self.graph.get_connections(id=post_id,
                                          connection_name='insights',
                                          metric=metric,
                                          period='lifetime',
                                          show_description_from_api_doc=True)

    def write_posts_insights_into_database(self, **task_data):
        year = task_data['year']
        month = task_data['month']
        year_str = str(year)
        month_str = str(month)
        page_id = task_data['page_id']
        posts = self.monthly_posts(page_id, year, month)

        # -------------------------------------------------- ACTIVITY --------------------------------------------------
        activity_value, activity_unique_value = list(), list()
        share, like, comment = list(), list(), list()
        share_unique, like_unique, comment_unique = list(), list(), list()

        # --------------------------------------------------- CLICKS ---------------------------------------------------
        clicks_value, clicks_unique_value = list(), list()
        clicks_by_type_video_play, clicks_by_type_other, clicks_by_type_link = list(), list(), list()

        # ------------------------------------------------- IMPRESSIONS ------------------------------------------------
        impressions_value, impressions_unique_value, post_impressions_organic_unique_value= list(), list() ,list()
        impressions_fan_value, impressions_fan_unique_value = list(), list()
        post_impressions_paid_unique_value, post_impressions_viral_value, post_impressions_organic_value, post_impressions_paid_value, page_engaged_users_value = list(), list() ,list() , list() ,list()

        # ------------------------------------------------- ENGAGEMENT -------------------------------------------------
        engaged_users_value, negative_feedback_value, negative_feedback_unique_value = list(), list(), list()

        # -------------------------------------------------- REACTIONS -------------------------------------------------
        reactions_like_total_value, reactions_love_total_value, reactions_wow_total_value = list(), list(), list()
        reactions_haha_total_value, reactions_sorry_total_value, reactions_anger_total_value = list(), list(), list()

        # ---------------------------------------------------- VIDEO ---------------------------------------------------
        video_avg_time_watched_value, video_complete_views_organic_value = list(), list()
        video_complete_views_organic_unique_value, video_views_organic_value = list(), list()
        video_views_organic_unique_value, video_views_value = list(), list()
        video_views_unique_value, video_view_time_value, video_complete_views_paid_value = list(), list(), list()
        video_complete_views_paid_value, video_complete_views_paid_unique_value, video_retention_graph_autoplayed_value, video_views_autoplayed_value, video_retention_graph_value, video_retention_graph_clicked_to_play_value, video_views_organic_unique_value, video_views_paid_value, video_views_paid_unique_value, video_length_value, video_views_clicked_to_play_value, video_views_10s_value, video_views_10s_unique_value, video_views_10s_autoplayed_value, video_views_10s_clicked_to_play_value, video_views_10s_organic_value, video_views_10s_paid_value, video_views_10s_sound_on_value, video_views_sound_on_value, video_view_time_organic_value, video_view_time_by_age_bucket_and_gender_value, video_view_time_by_region_id_value, video_views_by_distribution_type_value, video_view_time_by_distribution_type_value, video_view_time_by_country_id_value = list(),list(),list(),list(),list(),list(),list(),list(),list(),list(),list(),list(),list(),list(),list(),list(),list(),list(),list(),list(),list(),list(),list(),list(),list()
        video_complete_views_30s_clicked_to_play_value, video_complete_views_30s_autoplayed_value = list(), list()

        for ind, post in enumerate(posts):
            post_id = post['id']
            created_time = post['created_time'][:16]
            post_type = post['type']

            try:
                post_name = post['name']
            except:
                post_name = 'None'
                
            try:
                post_message = post['message']
            except:
                post_message = 'None'

            try:
                post_link = post['link']
            except:
                post_link = 'None'
            try:
                is_hidden = post['is_hidden']
            except:
                is_hidden = True

            pool_gen = ThreadPool(processes=61)
            
            # ------------------------------------------------ COMMENTS ------------------------------------------------
            comment_value_request=pool_gen.apply_async(self.save_coments,(post,))
            comment_value = comment_value_request.get()

            # ------------------------------------------------ ACTIVITY ------------------------------------------------
            # The number of stories generated about your Page post. (Total Count)
            activity_value_request=pool_gen.apply_async(self.get_activities,(post_id,))
            activity_value = activity_value_request.get()

            # The number of unique people who created a story by interacting with your Page post. (Unique Users)
            activity_unique_request=pool_gen.apply_async(self.get_activities_unique,(post_id,))
            activity_unique_value = activity_unique_request.get()

            # # The number of stories created about your Page post, by action type. (Total Count)
            activity_by_action_type_request=pool_gen.apply_async(self.get_activity_by_action_type,(post_id,))
            share, like, comment = activity_by_action_type_request.get()

            # # The number of unique people who created a story about your Page post by interacting with it. (Unique Users)

            activity_by_action_type_unique_request=pool_gen.apply_async(self.get_activity_by_action_type_unique,(post_id,))
            share_unique, like_unique, comment_unique = activity_by_action_type_unique_request.get()

            # # ------------------------------------------------- CLICKS -------------------------------------------------
            # # The number of clicks anywhere in your post on News Feed from the user that matched the audience targeting on it.
            # # (Total Count)
            post_clicks_request=pool_gen.apply_async(self.get_post_clicks,(post_id,))
            clicks_value = post_clicks_request.get()

            # # The number of clicks anywhere in your post on News Feed from the user that matched the audience targeting on it.
            # # (Total Count)
            pool_click_unique_request=pool_gen.apply_async(self.get_post_clicks_unique,(post_id,))
            clicks_unique_value = pool_click_unique_request.get()

            # # The number of clicks anywhere in the post on News Feed from users that matched the audience targeting on the post,
            # #  by type. (Total Count)
            pool_clicks_by_type_request=pool_gen.apply_async(self.get_clicks_by_type,(post_id,))
            clicks_by_type_video_play, clicks_by_type_other, clicks_by_type_link = pool_clicks_by_type_request.get()

            # # ---------------------------------------------- IMPRESSIONS -----------------------------------------------
            # # The number of times your Page's post entered a person's screen. Posts include statuses, photos, links, videos
            # # and more. (Total Count)
            pool_impressions_request=pool_gen.apply_async(self.get_impressions,(post_id,))
            impressions_value = pool_impressions_request.get()

            # # The number of people who had your Page's post enter their screen. Posts include statuses, photos, links, videos
            # # and more. (Unique Users)
            pool_impressions_unique_request=pool_gen.apply_async(self.get_impressions_unique,(post_id,))
            impressions_unique_value = pool_impressions_unique_request.get()

            # # The number of impressions of your Page post to people who have liked your Page. (Total Count)
            pool_impressions_fan_request=pool_gen.apply_async(self.get_impressions_fan,(post_id,))
            impressions_fan_value = pool_impressions_fan_request.get()

            # # The number of people who saw your Page post because they've liked your Page (Unique Users)
            pool_impressions_fan_unique_request=pool_gen.apply_async(self.get_impressions_fan_unique,(post_id,))
            impressions_fan_unique_value = pool_impressions_fan_unique_request.get()
            
            #Post organic value
            impressions_organic_unique_request=pool_gen.apply_async(self.get_impressions_organic_unique,(post_id,))
            post_impressions_organic_unique_value = impressions_organic_unique_request.get()

            #Post impressions paid
            impressions_paid_unique_value_request=pool_gen.apply_async(self.get_impressions_paid_unique_value,(post_id,))
            post_impressions_paid_unique_value = impressions_paid_unique_value_request.get()
            
            #Post impressions Viral
            impressions_viral_request=pool_gen.apply_async(self.get_impressions_viral,(post_id,))
            post_impressions_viral_value = impressions_viral_request.get()

            #Post impressions organic
            post_impressions_organic_request=pool_gen.apply_async(self.get_post_impressions_organic,(post_id,))
            post_impressions_organic_value = post_impressions_organic_request.get()

            #Post Impression Paid
            post_impressions_paid_request=pool_gen.apply_async(self.get_post_impressions_paid,(post_id,))
            post_impressions_paid_value = post_impressions_paid_request.get()
            
            #Page engaged User
            page_engaged_users_request=pool_gen.apply_async(self.get_page_engaged_users,(post_id,))
            page_engaged_users_value = page_engaged_users_request.get()
            
            # # ----------------------------------------------- ENGAGEMENT -----------------------------------------------
            # # The number of unique people who engaged in certain ways with your Page post, for example by commenting on, liking,
            # # sharing, or clicking upon particular elements of the post. (Unique Users)
            engaged_user_request=pool_gen.apply_async(self.get_engaged_user,(post_id,))
            engaged_users_value = engaged_user_request.get()

            # # The number of times people have given negative feedback to your post. (Total Count)        
            negative_feedback_request=pool_gen.apply_async(self.get_negative_feedback,(post_id,))
            negative_feedback_value = negative_feedback_request.get()

            # # The number of people who have given negative feedback to your post. (Unique Users)
            negative_feedback_unique_request=pool_gen.apply_async(self.get_negative_feedback_unique,(post_id,))
            negative_feedback_unique_value = negative_feedback_unique_request.get()

            # # ----------------------------------------------- REACTIONS ------------------------------------------------
            # # Total like reactions of a post
            reactions_like_total_request=pool_gen.apply_async(self.get_reactions_like_total,(post_id,))
            reactions_like_total_value = reactions_like_total_request.get()

            # # Total love reactions of a post      
            reactions_love_total_request=pool_gen.apply_async(self.get_reactions_love_total,(post_id,))
            reactions_love_total_value = reactions_love_total_request.get()

            # # Total wow reactions of a post
            reactions_wow_total_request=pool_gen.apply_async(self.get_reactions_wow_total,(post_id,))
            reactions_wow_total_value = reactions_wow_total_request.get()
            
            # # Total haha reactions of a post
            reactions_haha_total_request=pool_gen.apply_async(self.get_reactions_haha_total,(post_id,))
            reactions_haha_total_value = reactions_haha_total_request.get()
            
            # # Total sorry reactions of a post
            reactions_sorry_total_request=pool_gen.apply_async(self.get_reactions_sorry_total,(post_id,))
            reactions_sorry_total_value = reactions_sorry_total_request.get()

            # # Total anger reactions of a post
            reactions_anger_total_request=pool_gen.apply_async(self.get_reactions_anger_total,(post_id,))
            reactions_anger_total_value = reactions_anger_total_request.get()

            # # -------------------------------------------------- VIDEO -------------------------------------------------
            if post_type == 'video':

                # Average time (in ms) video viewed (Total Count)
                video_avg_time_watched_request = pool_gen.apply_async(self.get_video_avg_time_watched,(post_id,))
                video_avg_time_watched_value = video_avg_time_watched_request.get()

                # Number of times your video was viewed to 95% of its length without any paid promotion. (Total Count)
                video_complete_views_organic_request=pool_gen.apply_async(self.get_video_complete_views_organic,(post_id,))
                video_complete_views_organic_value = video_complete_views_organic_request.get()

                # Number of times your video was viewed to 95% of its length without any paid promotion. (Unique Users)
                video_complete_views_organic_unique_request=pool_gen.apply_async(self.get_video_complete_views_organic_unique,(post_id,))
                video_complete_views_organic_unique_value = video_complete_views_organic_unique_request.get()

                # Number of times your video was viewed to 95% of its length without any paid promotion. (Unique Users)
                video_views_organic_request=pool_gen.apply_async(self.get_video_views_organic,(post_id,))
                video_views_organic_value = video_views_organic_request.get()

                # Number of times your video was viewed for more than 3 seconds without any paid promotion. (Unique Users)
                video_views_organic_unique_request=pool_gen.apply_async(self.get_video_views_organic_unique,(post_id,))
                video_views_organic_unique_value = video_views_organic_unique_request.get()
                
                # Total number of times your video was viewed for more than 3 seconds. (Total Count)
                video_views_request=pool_gen.apply_async(self.get_video_views,(post_id,))
                video_views_value = video_views_request.get()
                
                # Number of unique people who viewed your video for more than 3 seconds. (Unique Users)
                video_views_unique_request=pool_gen.apply_async(self.get_video_views_unique,(post_id,))
                video_views_unique_value = video_views_unique_request.get()
                
                # Total time (in ms) video has been viewed (Total Count)
                video_view_time_request=pool_gen.apply_async(self.get_video_view_time,(post_id,))
                video_view_time_value = video_view_time_request.get()

                #Video post_video_complete_views_paid
                video_complete_views_paid_request=pool_gen.apply_async(self.get_video_complete_views_paid,(post_id,))
                video_complete_views_paid_value = video_complete_views_paid_request.get()

                video_complete_views_paid_request = pool_gen.apply_async(self.get_video_complete_views_paid,(post_id,))
                video_complete_views_paid_value = video_complete_views_paid_request.get()

                video_complete_views_paid_unique_request = pool_gen.apply_async(self.get_post_video_complete_views_paid_unique,(post_id,))
                video_complete_views_paid_unique_value = video_complete_views_paid_unique_request.get()

                video_retention_graph_autoplayed_request = pool_gen.apply_async(self.get_post_video_retention_graph_autoplayed,(post_id,))
                video_retention_graph_autoplayed_value = video_retention_graph_autoplayed_request.get()

                video_views_autoplayed_request = pool_gen.apply_async(self.get_post_video_views_autoplayed,(post_id,))
                video_views_autoplayed_value = video_views_autoplayed_request.get()

                video_retention_graph_request = pool_gen.apply_async(self.get_post_video_retention_graph,(post_id,))
                video_retention_graph_value = video_retention_graph_request.get()

                video_retention_graph_clicked_to_play_request = pool_gen.apply_async(self.get_post_video_retention_graph_clicked_to_play,(post_id,))
                video_retention_graph_clicked_to_play_value = video_retention_graph_clicked_to_play_request.get()

                video_views_organic_unique_request = pool_gen.apply_async(self.get_post_video_views_organic_unique,(post_id,))
                video_views_organic_unique_value = video_views_organic_unique_request.get()

                video_views_paid_request = pool_gen.apply_async(self.get_post_video_views_paid,(post_id,))
                video_views_paid_value = video_views_paid_request.get()

                video_views_paid_unique_request = pool_gen.apply_async(self.get_post_video_views_paid_unique,(post_id,))
                video_views_paid_unique_value = video_views_paid_unique_request.get()

                video_length_request = pool_gen.apply_async(self.get_post_video_length,(post_id,))
                video_length_value = video_length_request.get()

                video_views_clicked_to_play_request = pool_gen.apply_async(self.get_post_video_views_clicked_to_play,(post_id,))
                video_views_clicked_to_play_value = video_views_clicked_to_play_request.get()

                video_views_10s_request = pool_gen.apply_async(self.get_post_video_views_10s,(post_id,))
                video_views_10s_value = video_views_10s_request.get()
 

                video_views_10s_unique_request = pool_gen.apply_async(self.get_post_video_views_10s_unique,(post_id,))
                video_views_10s_unique_value = video_views_10s_unique_request.get()

                video_views_10s_autoplayed_request = pool_gen.apply_async(self.get_post_video_views_10s_autoplayed,(post_id,))
                video_views_10s_autoplayed_value = video_views_10s_autoplayed_request.get()

                video_views_10s_clicked_to_play_request = pool_gen.apply_async(self.get_post_video_views_10s_clicked_to_play,(post_id,))
                video_views_10s_clicked_to_play_value = video_views_10s_clicked_to_play_request.get()

                video_views_10s_organic_request = pool_gen.apply_async(self.get_post_video_views_10s_organic,(post_id,))
                video_views_10s_organic_value = video_views_10s_organic_request.get()

                video_views_10s_paid_request = pool_gen.apply_async(self.get_post_video_views_10s_paid,(post_id,))
                video_views_10s_paid_value = video_views_10s_paid_request.get()

                video_views_10s_sound_on_request = pool_gen.apply_async(self.get_post_video_views_10s_sound_on,(post_id,))
                video_views_10s_sound_on_value = video_views_10s_sound_on_request.get()

                video_views_sound_on_request = pool_gen.apply_async(self.get_post_video_views_sound_on,(post_id,))
                video_views_sound_on_value = video_views_sound_on_request.get()

                video_view_time_organic_request = pool_gen.apply_async(self.get_post_video_view_time_organic,(post_id,))
                video_view_time_organic_value = video_view_time_organic_request.get()

                video_view_time_by_age_bucket_and_gender_request = pool_gen.apply_async(self.get_post_video_view_time_by_age_bucket_and_gender,(post_id,))
                video_view_time_by_age_bucket_and_gender_value = video_view_time_by_age_bucket_and_gender_request.get()

                video_view_time_by_region_id_request = pool_gen.apply_async(self.get_post_video_view_time_by_region_id,(post_id,))
                video_view_time_by_region_id_value = video_view_time_by_region_id_request.get()

                video_views_by_distribution_type_request = pool_gen.apply_async(self.get_post_video_views_by_distribution_type,(post_id,))
                video_views_by_distribution_type_value = video_views_by_distribution_type_request.get()

                video_view_time_by_distribution_type_request = pool_gen.apply_async(self.get_post_video_view_time_by_distribution_type,(post_id,))
                video_view_time_by_distribution_type_value = video_view_time_by_distribution_type_request.get()

                video_view_time_by_country_id_request = pool_gen.apply_async(self.get_post_video_view_time_by_country_id,(post_id,))
                video_view_time_by_country_id_value = video_view_time_by_country_id_request.get()

                video_complete_views_30s_clicked_to_play_request = pool_gen.apply_async(self.get_post_video_complete_views_30s_clicked_to_play,(post_id,))
                video_complete_views_30s_clicked_to_play_value = video_complete_views_30s_clicked_to_play_request.get()

                video_complete_views_30s_autoplayed_request = pool_gen.apply_async(self.get_post_video_complete_views_30s_autoplayed,(post_id,))
                video_complete_views_30s_autoplayed_value = video_complete_views_30s_autoplayed_request.get()

            else:
                video_avg_time_watched_value = 0
                video_complete_views_organic_value = 0
                video_complete_views_organic_unique_value = 0
                video_views_organic_value = 0
                video_views_organic_unique_value = 0
                video_views_value = 0
                video_views_unique_value = 0
                video_view_time_value = 0
                video_complete_views_paid_value = 0
                video_complete_views_paid_unique_value = 0
                video_retention_graph_autoplayed_value = 0
                video_views_autoplayed_value = 0
                video_retention_graph_value = 0
                video_retention_graph_clicked_to_play_value = 0
                video_views_organic_unique_value = 0
                video_views_paid_value = 0
                video_views_paid_unique_value = 0
                video_length_value = 0
                video_views_clicked_to_play_value = 0
                video_views_10s_value = 0
                video_views_10s_unique_value = 0
                video_views_10s_autoplayed_value = 0
                video_views_10s_clicked_to_play_value = 0
                video_views_10s_organic_value = 0
                video_views_10s_paid_value = 0
                video_views_10s_sound_on_value = 0
                video_views_sound_on_value = 0
                video_view_time_organic_value = 0
                video_view_time_by_age_bucket_and_gender_value = 0
                video_view_time_by_region_id_value = 0
                video_views_by_distribution_type_value = 0
                video_view_time_by_distribution_type_value = 0
                video_view_time_by_country_id_value = 0
                video_complete_views_30s_clicked_to_play_value = 0
                video_complete_views_30s_autoplayed_value = 0

            # # -------------------------------------------------- SAVE IN DATABASE -------------------------------------------------
            
            data_to_database = {}
            data_to_database['post_id'] = post_id
            data_to_database['post_message'] = post_message
            data_to_database['is_hidden'] = is_hidden
            data_to_database['post_link'] = post_link
            data_to_database['id_page'] = page_id
            data_to_database['year_data'] = year_str
            data_to_database['month_data'] = month_str 
            data_to_database['comment_num'] = comment_value
            data_to_database['activity'] = activity_value
            data_to_database['activity_by_action_type_comment'] = comment
            data_to_database['activity_by_action_type_like'] = like
            data_to_database['activity_by_action_type_share'] = share
            data_to_database['activity_by_action_type_unique_comment'] = comment_unique
            data_to_database['activity_by_action_type_unique_like'] = like_unique
            data_to_database['activity_by_action_type_unique_share'] = share_unique
            data_to_database['activity_unique'] = activity_unique_value
            data_to_database['clicks'] = clicks_value
            data_to_database['clicks_by_type_link'] = clicks_by_type_link
            data_to_database['clicks_by_type_other'] = clicks_by_type_other
            data_to_database['clicks_by_type_video_play'] = clicks_by_type_video_play
            data_to_database['clicks_unique'] = clicks_unique_value
            data_to_database['engaged_users'] = engaged_users_value
            data_to_database['impressions'] = impressions_value
            data_to_database['impressions_fan'] = impressions_fan_value
            data_to_database['impressions_fan_unique'] = impressions_fan_unique_value
            data_to_database['impressions_unique'] = impressions_unique_value
            data_to_database['negative_feedback'] = negative_feedback_value
            data_to_database['negative_feedback_unique'] = negative_feedback_unique_value
            data_to_database['page_engaged_users'] = page_engaged_users_value
            data_to_database['post_created_time'] = created_time
            data_to_database['post_impressions_organic'] = post_impressions_organic_value
            data_to_database['post_impressions_organic_unique'] = post_impressions_organic_unique_value
            data_to_database['post_impressions_paid'] = post_impressions_paid_value
            data_to_database['post_impressions_paid_unique'] = post_impressions_paid_unique_value
            data_to_database['post_impressions_viral'] = post_impressions_viral_value
            data_to_database['post_name'] = post_name
            data_to_database['post_type'] = post_type
            data_to_database['reactions_anger'] = reactions_anger_total_value
            data_to_database['reactions_haha'] = reactions_haha_total_value
            data_to_database['reactions_like'] = reactions_like_total_value
            data_to_database['reactions_love'] = reactions_love_total_value
            data_to_database['reactions_sorry'] = reactions_sorry_total_value
            data_to_database['reactions_wow'] = reactions_wow_total_value
            data_to_database['video_avg_time_watched'] = video_avg_time_watched_value
            data_to_database['video_complete_views_organic'] = video_complete_views_organic_value
            data_to_database['video_complete_views_organic_unique'] = video_complete_views_organic_unique_value
            data_to_database['video_organic_unique'] = video_views_organic_unique_value
            data_to_database['video_view_time'] = video_view_time_value
            data_to_database['video_views'] = video_views_value
            data_to_database['video_views_organic'] = video_views_organic_value
            data_to_database['video_views_unique'] = video_views_unique_value
            data_to_database['video_complete_views_paid'] = video_complete_views_paid_value
            data_to_database['video_complete_views_paid_unique'] = video_complete_views_paid_unique_value
            data_to_database['video_retention_graph_autoplayed'] = video_retention_graph_autoplayed_value
            data_to_database['video_views_autoplayed'] = video_views_autoplayed_value
            data_to_database['video_retention_graph'] = video_retention_graph_value
            data_to_database['video_retention_graph_clicked_to_play'] = video_retention_graph_clicked_to_play_value
            data_to_database['video_views_organic_unique'] = video_views_organic_unique_value
            data_to_database['video_views_paid'] = video_views_paid_value
            data_to_database['video_views_paid_unique'] = video_views_paid_unique_value
            data_to_database['video_length'] = video_length_value
            data_to_database['video_views_clicked_to_play'] = video_views_clicked_to_play_value
            data_to_database['video_views_10s'] = video_views_10s_value
            data_to_database['video_views_10s_unique'] = video_views_10s_unique_value
            data_to_database['video_views_10s_autoplayed'] = video_views_10s_autoplayed_value
            data_to_database['video_views_10s_clicked_to_play'] = video_views_10s_clicked_to_play_value
            data_to_database['video_views_10s_organic'] = video_views_10s_organic_value
            data_to_database['video_views_10s_paid'] = video_views_10s_paid_value
            data_to_database['video_views_10s_sound_on'] = video_views_10s_sound_on_value
            data_to_database['video_views_sound_on'] = video_views_sound_on_value
            data_to_database['video_view_time_organic'] = video_view_time_organic_value
            data_to_database['video_view_time_by_age_bucket_and_gender'] = video_view_time_by_age_bucket_and_gender_value
            data_to_database['video_view_time_by_region_id'] = video_view_time_by_region_id_value
            data_to_database['video_views_by_distribution_type'] = video_views_by_distribution_type_value
            data_to_database['video_view_time_by_distribution_type'] = video_view_time_by_distribution_type_value
            data_to_database['video_view_time_by_country_id'] = video_view_time_by_country_id_value
            data_to_database['video_complete_views_30s_autoplayed'] = video_complete_views_30s_autoplayed_value
            data_to_database['video_complete_views_30s_clicked_to_play'] = video_complete_views_30s_clicked_to_play_value

            self.database.insert('post',data_to_database)
            del data_to_database

            print(post_id, pool_gen)
            # # -------------------------------------------------- KILL THEARD -------------------------------------------------            
            time.sleep(5)
            pool_gen.close()
            pool_gen.join()
        
    #Activities
    def get_activities(self,tpost_id):
        activity = self.post_life_time_insights(tpost_id, 'post_activity')

        try:
            activity_value = activity['data'][0]['values'][0]['value']
        except:
            activity_value = 0   

        return activity_value
    
    def get_activities_unique(self,tpost_id):
        activity_unique = self.post_life_time_insights(tpost_id, 'post_activity_unique')
        try:
            activity_unique_value = activity_unique['data'][0]['values'][0]['value']
        except:
            activity_unique_value = 0
    
        return activity_unique_value

    def get_activity_by_action_type(self,tpost_id):
        
        activity_by_action_type = self.post_life_time_insights(tpost_id, 'post_activity_by_action_type')
        try:
            share = activity_by_action_type['data'][0]['values'][0]['value']['share']
        except:
            share = 0
        try:
            like = activity_by_action_type['data'][0]['values'][0]['value']['like']
        except:
            like = 0
        try:
            comment = activity_by_action_type['data'][0]['values'][0]['value']['comment']
        except:
            comment = 0
        
        return share, like, comment

    def get_activity_by_action_type_unique(self,tpost_id):

        activity_by_action_type_unique = self.post_life_time_insights(tpost_id, 'post_activity_by_action_type_unique')
        try:
            share_unique = activity_by_action_type_unique['data'][0]['values'][0]['value']['share']
        except:
            share_unique = 0

        try:
            like_unique = activity_by_action_type_unique['data'][0]['values'][0]['value']['like']
        except:
            like_unique = 0

        try:
            comment_unique = activity_by_action_type_unique['data'][0]['values'][0]['value']['comment']
        except:
            comment_unique = 0
        
        return share_unique, like_unique, comment_unique
    
    #Clicks
    def get_post_clicks(self,tpost_id):
        
        clicks = self.post_life_time_insights(tpost_id, 'post_clicks')
        try:
            clicks_value = clicks['data'][0]['values'][0]['value']
        except:
            clicks_value = 0
        
        return clicks_value

    def get_post_clicks_unique(self,tpost_id):

        clicks_unique = self.post_life_time_insights(tpost_id, 'post_clicks_unique')
        try:
            clicks_unique_value = clicks_unique['data'][0]['values'][0]['value']
        except:
            clicks_unique_value = 0
        
        return clicks_unique_value

    def get_clicks_by_type(self,tpost_id):

        clicks_by_type_video_play, clicks_by_type_other, clicks_by_type_link = list(), list(), list()
        clicks_by_type = self.post_life_time_insights(tpost_id, 'post_clicks_by_type')
        try:
            clicks_by_type_video_play = clicks_by_type['data'][0]['values'][0]['value']['video play']
        except:
            clicks_by_type_video_play = 0
        try:
            clicks_by_type_other = clicks_by_type['data'][0]['values'][0]['value']['other clicks']
        except:
            clicks_by_type_other = 0
        try:
            clicks_by_type_link = clicks_by_type['data'][0]['values'][0]['value']['link clicks']
        except:
            clicks_by_type_link = 0
        
        return clicks_by_type_video_play, clicks_by_type_other, clicks_by_type_link

    #Impression
    def get_impressions(self,tpost_id):
        
        impressions = self.post_life_time_insights(tpost_id, 'post_impressions')
        try:
            impressions_value = impressions['data'][0]['values'][0]['value']
        except:
            impressions_value = 0
        
        return impressions_value

    def get_impressions_unique(self,tpost_id):
        
        impressions_unique = self.post_life_time_insights(tpost_id, 'post_impressions_unique')
        try:
            impressions_unique_value = impressions_unique['data'][0]['values'][0]['value']
        except:
            impressions_unique_value = 0
        
        return impressions_unique_value
    
    def get_impressions_fan(self,tpost_id):
        
        impressions_fan = self.post_life_time_insights(tpost_id, 'post_impressions_fan')
        try:
            impressions_fan_value = impressions_fan['data'][0]['values'][0]['value']
        except:
            impressions_fan_value = 0
        
        return impressions_fan_value
    
    def get_impressions_fan_unique(self,tpost_id):
        
        impressions_fan_unique = self.post_life_time_insights(tpost_id, 'post_impressions_fan_unique')
        try:
            impressions_fan_unique_value = impressions_fan_unique['data'][0]['values'][0]['value']
        except:
            impressions_fan_unique_value = 0
        
        return impressions_fan_unique_value
    
    def get_impressions_organic_unique(self,tpost_id):
        
        post_impressions_organic_unique = self.post_life_time_insights(tpost_id, 'post_impressions_organic_unique')
        try:
            post_impressions_organic_unique_value = post_impressions_organic_unique['data'][0]['values'][0]['value']
        except:
            post_impressions_organic_unique_value = 0
        
        return post_impressions_organic_unique_value
    
    def get_impressions_paid_unique_value(self,tpost_id):
        
        post_impressions_paid_unique = self.post_life_time_insights(tpost_id, 'post_impressions_paid_unique')
        try:
            post_impressions_paid_unique_value = post_impressions_paid_unique['data'][0]['values'][0]['value']
        except:
            post_impressions_paid_unique_value = 0
        
        return post_impressions_paid_unique_value
    
    def get_impressions_viral(self,tpost_id):
        
        post_impressions_viral = self.post_life_time_insights(tpost_id, 'post_impressions_viral')
        try:
            post_impressions_viral_value = post_impressions_viral['data'][0]['values'][0]['value']
        except:
            post_impressions_viral_value = 0
        
        return post_impressions_viral_value
    
    def get_post_impressions_organic(self,tpost_id):
        
        post_impressions_organic = self.post_life_time_insights(tpost_id, 'post_impressions_organic')
        try:
            post_impressions_organic_value = post_impressions_organic['data'][0]['values'][0]['value']
        except:
            post_impressions_organic_value = 0
        
        return post_impressions_organic_value

    def get_post_impressions_paid(self,tpost_id):
        
        post_impressions_paid = self.post_life_time_insights(tpost_id, 'post_impressions_paid')
        try:
            post_impressions_paid_value = post_impressions_paid['data'][0]['values'][0]['value']
        except:
            post_impressions_paid_value = 0
        
        return post_impressions_paid_value

    def get_page_engaged_users(self,tpost_id):
        
        page_engaged_users = self.post_life_time_insights(tpost_id, 'page_engaged_users')

        try:
            page_engaged_users_value = page_engaged_users['data'][0]['values'][0]['value']
        except:
            page_engaged_users_value = 0
        
        return page_engaged_users_value

    #Engagement

    def get_engaged_user(self,tpost_id):
        engaged_users_value= list()
        engaged_users = self.post_life_time_insights(tpost_id, 'post_engaged_users')
        try:
            engaged_users_value = engaged_users['data'][0]['values'][0]['value']
        except:
            engaged_users_value = 0
        
        return engaged_users_value

    def get_negative_feedback(self,tpost_id):
        
        negative_feedback = self.post_life_time_insights(tpost_id, 'post_negative_feedback')
        try:
            negative_feedback_value = negative_feedback['data'][0]['values'][0]['value']
        except:
            negative_feedback_value = 0
        
        return negative_feedback_value

    def get_negative_feedback_unique(self,tpost_id):
        
        negative_feedback_unique = self.post_life_time_insights(tpost_id, 'post_negative_feedback_unique')
        try:
            negative_feedback_unique_value = negative_feedback_unique['data'][0]['values'][0]['value']
        except:
            negative_feedback_unique_value = 0
        
        return negative_feedback_unique_value
    
    #REACTIONS
    def get_reactions_like_total(self,tpost_id):
        
        reactions_like_total = self.post_life_time_insights(tpost_id, 'post_reactions_like_total')
        try:
            reactions_like_total_value = reactions_like_total['data'][0]['values'][0]['value']
        except:
            reactions_like_total_value = 0
        
        return reactions_like_total_value
    
    def get_reactions_love_total(self,tpost_id):
        
        reactions_love_total = self.post_life_time_insights(tpost_id, 'post_reactions_love_total')
        try:
            reactions_love_total_value = reactions_love_total['data'][0]['values'][0]['value']
        except:
            reactions_love_total_value = 0
        
        return reactions_love_total_value
    
    def get_reactions_wow_total(self,tpost_id):
        
        reactions_wow_total = self.post_life_time_insights(tpost_id, 'post_reactions_wow_total')
        try:
            reactions_wow_total_value = reactions_wow_total['data'][0]['values'][0]['value']
        except:
            reactions_wow_total_value = 0
        
        return reactions_wow_total_value

    def get_reactions_haha_total(self,tpost_id):
        
        reactions_haha_total = self.post_life_time_insights(tpost_id, 'post_reactions_haha_total')
        try:
            reactions_haha_total_value = reactions_haha_total['data'][0]['values'][0]['value']
        except:
            reactions_haha_total_value = 0
        
        return reactions_haha_total_value
    
    def get_reactions_sorry_total(self,tpost_id):
        
        reactions_sorry_total = self.post_life_time_insights(tpost_id, 'post_reactions_sorry_total')
        try:
            reactions_sorry_total_value = reactions_sorry_total['data'][0]['values'][0]['value']
        except:
            reactions_sorry_total_value = 0
        
        return reactions_sorry_total_value
    
    def get_reactions_anger_total(self,tpost_id):
        
        reactions_anger_total = self.post_life_time_insights(tpost_id, 'post_reactions_anger_total')
        try:
            reactions_anger_total_value = reactions_anger_total['data'][0]['values'][0]['value']
        except:
            reactions_anger_total_value = 0
        
        return reactions_anger_total_value

    #Videos
    def get_video_avg_time_watched(self,tpost_id):
        
        video_avg_time_watched = self.post_life_time_insights(tpost_id, 'post_video_avg_time_watched')
        try:
            video_avg_time_watched_value = video_avg_time_watched['data'][0]['values'][0]['value']
        except:
            video_avg_time_watched_value = 0
        
        return video_avg_time_watched_value

    def get_video_complete_views_organic(self,tpost_id):
        
        video_complete_views_organic = self.post_life_time_insights(tpost_id, 'post_video_complete_views_organic')
        try:
            video_complete_views_organic_value = video_complete_views_organic['data'][0]['values'][0]['value']
        except:
            video_complete_views_organic_value = 0
        
        return video_complete_views_organic_value

    def get_video_complete_views_organic_unique(self,tpost_id):
        
        video_complete_views_organic_unique = self.post_life_time_insights(tpost_id,'post_video_complete_views_organic_unique')
        try:
            video_complete_views_organic_unique_value = video_complete_views_organic_unique['data'][0]['values'][0]['value']
        except:
            video_complete_views_organic_unique_value = 0
        
        return video_complete_views_organic_unique_value

    def get_video_views_organic(self,tpost_id):
        
        video_views_organic = self.post_life_time_insights(tpost_id, 'post_video_views_organic')
        try:
            video_views_organic_value = video_views_organic['data'][0]['values'][0]['value']
        except:
            video_views_organic_value = 0
        
        return video_views_organic_value

    def get_video_views_organic_unique(self,tpost_id):
        
        video_views_organic_unique = self.post_life_time_insights(tpost_id, 'post_video_views_organic_unique')
        try:
            video_views_organic_unique_value = video_views_organic_unique['data'][0]['values'][0]['value']
        except:
            video_views_organic_unique_value = 0
        
        return video_views_organic_unique_value

    def get_video_views(self,tpost_id):
        
        video_views = self.post_life_time_insights(tpost_id, 'post_video_views')
        try:
            video_views_value = video_views['data'][0]['values'][0]['value']
        except:
            video_views_value = 0
        
        return video_views_value

    def get_video_views_unique(self,tpost_id):
        
        video_views_unique = self.post_life_time_insights(tpost_id, 'post_video_views_unique')
        try:
            video_views_unique_value = video_views_unique['data'][0]['values'][0]['value']
        except:
            video_views_unique_value = 0
        
        return video_views_unique_value

    def get_video_view_time(self,tpost_id):
        
        video_view_time = self.post_life_time_insights(tpost_id, 'post_video_view_time')
        try:
            video_view_time_value = video_view_time['data'][0]['values'][0]['value']
        except:
            video_view_time_value = 0
        
        return video_view_time_value
    # Aqui empece
    def get_video_complete_views_paid(self,tpost_id):
        
        video_complete_views_paid = self.post_life_time_insights(tpost_id, 'post_video_complete_views_paid')
        try:
            video_complete_views_paid_value = video_complete_views_paid['data'][0]['values'][0]['value']

        except:
            video_complete_views_paid_value = 0
        
        return video_complete_views_paid_value
    
    def get_post_video_complete_views_paid(self, post_id):
        post_video_complete_views_paid = self.post_life_time_insights(post_id,'post_video_complete_views_paid')
        try:
            post_video_complete_views_paid_value = post_video_complete_views_paid['data'][0]['values'][0]['value']
        except:
            post_video_complete_views_paid_value = 0
        
        return post_video_complete_views_paid_value
        

    def get_post_video_complete_views_paid_unique(self, post_id):
        post_video_complete_views_paid_unique = self.post_life_time_insights(post_id,'post_video_complete_views_paid_unique')
        try:
            post_video_complete_views_paid_unique_value = post_video_complete_views_paid_unique['data'][0]['values'][0]['value']
        except:
            post_video_complete_views_paid_unique_value = 0
        
        return post_video_complete_views_paid_unique_value
        

    def get_post_video_retention_graph_autoplayed(self, post_id):
        post_video_retention_graph_autoplayed = self.post_life_time_insights(post_id,'post_video_retention_graph_autoplayed')
        try:
            post_video_retention_graph_autoplayed_value = post_video_retention_graph_autoplayed['data'][0]['values'][0]['value']
        except:
            post_video_retention_graph_autoplayed_value = 0
        
        return post_video_retention_graph_autoplayed_value
        
    def get_post_video_retention_graph(self, post_id):
        post_video_retention_graph = self.post_life_time_insights(post_id,'post_video_retention_graph')
        try:
            post_video_retention_graph_value = post_video_retention_graph['data'][0]['values'][0]['value']
        except:
            post_video_retention_graph_value = 0
        
        return post_video_retention_graph_value
      
    def get_post_video_retention_graph_clicked_to_play(self, post_id):
        post_video_retention_graph_clicked_to_play = self.post_life_time_insights(post_id,'post_video_retention_graph_clicked_to_play')
        try:
            post_video_retention_graph_clicked_to_play_value = post_video_retention_graph_clicked_to_play['data'][0]['values'][0]['value']
        except:
            post_video_retention_graph_clicked_to_play_value = 0
        
        return post_video_retention_graph_clicked_to_play_value

    def get_post_video_views_organic(self, post_id):
        post_video_views_organic = self.post_life_time_insights(post_id,'post_video_views_organic')
        try:
            post_video_views_organic_value = post_video_views_organic['data'][0]['values'][0]['value']
        except:
            post_video_views_organic_value = 0
        
        return post_video_views_organic_value
        

    def get_post_video_views_organic_unique(self, post_id):
        post_video_views_organic_unique = self.post_life_time_insights(post_id,'post_video_views_organic_unique')
        try:
            post_video_views_organic_unique_value = post_video_views_organic_unique	['data'][0]['values'][0]['value']
        except:
            post_video_views_organic_unique_value = 0
        
        return post_video_views_organic_unique_value
        

    def get_post_video_views_paid(self, post_id):
        post_video_views_paid = self.post_life_time_insights(post_id,'post_video_views_paid')
        try:
            post_video_views_paid_value = post_video_views_paid	['data'][0]['values'][0]['value']
        except:
            post_video_views_paid_value = 0
        
        return post_video_views_paid_value
        

    def get_post_video_views_paid_unique(self, post_id):
        post_video_views_paid_unique = self.post_life_time_insights(post_id,'post_video_views_paid_unique')
        try:
            post_video_views_paid_unique_value = post_video_views_paid_unique	['data'][0]['values'][0]['value']
        except:
            post_video_views_paid_unique_value = 0
        
        return post_video_views_paid_unique_value
        

    def get_post_video_length(self, post_id):
        post_video_length = self.post_life_time_insights(post_id,'post_video_length')
        try:
            post_video_length_value = post_video_length['data'][0]['values'][0]['value']
        except:
            post_video_length_value = 0
        
        return post_video_length_value
        

    def get_post_video_views_autoplayed(self, post_id):
        post_video_views_autoplayed = self.post_life_time_insights(post_id,'post_video_views_autoplayed')
        try:
            post_video_views_autoplayed_value = post_video_views_autoplayed	['data'][0]['values'][0]['value']
        except:
            post_video_views_autoplayed_value = 0
        
        return post_video_views_autoplayed_value
        

    def get_post_video_views_clicked_to_play(self, post_id):
        post_video_views_clicked_to_play = self.post_life_time_insights(post_id,'post_video_views_clicked_to_play')
        try:
            post_video_views_clicked_to_play_value = post_video_views_clicked_to_play	['data'][0]['values'][0]['value']
        except:
            post_video_views_clicked_to_play_value = 0
        
        return post_video_views_clicked_to_play_value
        

    def get_post_video_views_10s(self, post_id):
        post_video_views_10s = self.post_life_time_insights(post_id,'post_video_views_10s')
        try:
            post_video_views_10s_value = post_video_views_10s['data'][0]['values'][0]['value']
        except:
            post_video_views_10s_value = 0
        
        return post_video_views_10s_value
        

    def get_post_video_views_10s_unique(self, post_id):
        post_video_views_10s_unique = self.post_life_time_insights(post_id,'post_video_views_10s_unique')
        try:
            post_video_views_10s_unique_value = post_video_views_10s_unique	['data'][0]['values'][0]['value']
        except:
            post_video_views_10s_unique_value = 0
        
        return post_video_views_10s_unique_value
        

    def get_post_video_views_10s_autoplayed(self, post_id):
        post_video_views_10s_autoplayed = self.post_life_time_insights(post_id,'post_video_views_10s_autoplayed')
        try:
            post_video_views_10s_autoplayed_value = post_video_views_10s_autoplayed	['data'][0]['values'][0]['value']
        except:
            post_video_views_10s_autoplayed_value = 0
        
        return post_video_views_10s_autoplayed_value
        

    def get_post_video_views_10s_clicked_to_play(self, post_id):
        post_video_views_10s_clicked_to_play = self.post_life_time_insights(post_id,'post_video_views_10s_clicked_to_play')
        try:
            post_video_views_10s_clicked_to_play_value = post_video_views_10s_clicked_to_play	['data'][0]['values'][0]['value']
        except:
            post_video_views_10s_clicked_to_play_value = 0
        
        return post_video_views_10s_clicked_to_play_value
        

    def get_post_video_views_10s_organic(self, post_id):
        post_video_views_10s_organic = self.post_life_time_insights(post_id,'post_video_views_10s_organic')
        try:
            post_video_views_10s_organic_value = post_video_views_10s_organic	['data'][0]['values'][0]['value']
        except:
            post_video_views_10s_organic_value = 0
        
        return post_video_views_10s_organic_value
        

    def get_post_video_views_10s_paid(self, post_id):
        post_video_views_10s_paid = self.post_life_time_insights(post_id,'post_video_views_10s_paid')
        try:
            post_video_views_10s_paid_value = post_video_views_10s_paid	['data'][0]['values'][0]['value']
        except:
            post_video_views_10s_paid_value = 0
        
        return post_video_views_10s_paid_value
        

    def get_post_video_views_10s_sound_on(self, post_id):
        post_video_views_10s_sound_on = self.post_life_time_insights(post_id,'post_video_views_10s_sound_on')
        try:
            post_video_views_10s_sound_on_value = post_video_views_10s_sound_on['data'][0]['values'][0]['value']
        except:
            post_video_views_10s_sound_on_value = 0
        
        return post_video_views_10s_sound_on_value
        

    def get_post_video_views_sound_on(self, post_id):
        post_video_views_sound_on = self.post_life_time_insights(post_id,'post_video_views_sound_on')
        try:
            post_video_views_sound_on_value = post_video_views_sound_on['data'][0]['values'][0]['value']
        except:
            post_video_views_sound_on_value = 0
        
        return post_video_views_sound_on_value
        

    def get_post_video_view_time_organic(self, post_id):
        post_video_view_time_organic = self.post_life_time_insights(post_id,'post_video_view_time_organic')
        try:
            post_video_view_time_organic_value = post_video_view_time_organic['data'][0]['values'][0]['value']
        except:
            post_video_view_time_organic_value = 0
        
        return post_video_view_time_organic_value
        

    def get_post_video_view_time_by_age_bucket_and_gender(self, post_id):
        post_video_view_time_by_age_bucket_and_gender = self.post_life_time_insights(post_id,'post_video_view_time_by_age_bucket_and_gender')
        try:
            post_video_view_time_by_age_bucket_and_gender_value = post_video_view_time_by_age_bucket_and_gender['data'][0]['values'][0]['value']
        except:
            post_video_view_time_by_age_bucket_and_gender_value = 0
        
        return post_video_view_time_by_age_bucket_and_gender_value
        

    def get_post_video_view_time_by_region_id(self, post_id):
        post_video_view_time_by_region_id = self.post_life_time_insights(post_id,'post_video_view_time_by_region_id')
        try:
            post_video_view_time_by_region_id_value = post_video_view_time_by_region_id['data'][0]['values'][0]['value']
        except:
            post_video_view_time_by_region_id_value = 0
        
        return post_video_view_time_by_region_id_value
        

    def get_post_video_views_by_distribution_type(self, post_id):
        post_video_views_by_distribution_type = self.post_life_time_insights(post_id,'post_video_views_by_distribution_type')
        try:
            post_video_views_by_distribution_type_value = post_video_views_by_distribution_type['data'][0]['values'][0]['value']
        except:
            post_video_views_by_distribution_type_value = 0
        
        return post_video_views_by_distribution_type_value
        

    def get_post_video_view_time_by_distribution_type(self, post_id):
        post_video_view_time_by_distribution_type = self.post_life_time_insights(post_id,'post_video_view_time_by_distribution_type')
        try:
            post_video_view_time_by_distribution_type_value = post_video_view_time_by_distribution_type['data'][0]['values'][0]['value']
        except:
            post_video_view_time_by_distribution_type_value = 0
        
        return post_video_view_time_by_distribution_type_value
        

    def get_post_video_view_time_by_country_id(self, post_id):
        post_video_view_time_by_country_id = self.post_life_time_insights(post_id,'post_video_view_time_by_country_id')
        try:
            post_video_view_time_by_country_id_value = post_video_view_time_by_country_id['data'][0]['values'][0]['value']
        except:
            post_video_view_time_by_country_id_value = 0
        
        return post_video_view_time_by_country_id_value
    
    def get_post_video_complete_views_30s_autoplayed(self, post_id):
        get_post_video_complete_views_30s_autoplayed = self.post_life_time_insights(post_id,'post_video_complete_views_30s_autoplayed')

        try:
            get_post_video_complete_views_30s_autoplayed_value = get_post_video_complete_views_30s_autoplayed['data'][0]['values'][0]['value']
        except:
            get_post_video_complete_views_30s_autoplayed_value = 0
        
        return get_post_video_complete_views_30s_autoplayed_value

    def get_post_video_complete_views_30s_clicked_to_play(self, post_id):
        post_video_complete_views_30s_clicked_to_play = self.post_life_time_insights(post_id,'post_video_complete_views_30s_clicked_to_play')
        try:
            post_video_complete_views_30s_clicked_to_play_value = post_video_complete_views_30s_clicked_to_play['data'][0]['values'][0]['value']
        except:
            post_video_complete_views_30s_clicked_to_play_value = 0
        
        return post_video_complete_views_30s_clicked_to_play_value
        
    def save_coments(self,post_data):
        post_id = post_data['id'] 
        post_coments = self.database.select('comment','post_id',post_id)
        num_comments = 0

        if len(post_coments) > 0:
            where_condition = post_id='+str(post_id)+ '
            self.database.delete('post_comment_data', where_condition)

        try :
            comments_data = post_data['comments']['data']
            for coment in comments_data:
                if(coment['message'] != ''):
                    data_to_database = {}
                    data_to_database['comment_id'] = coment['id']
                    data_to_database['post_id'] = post_id
                    data_to_database['message'] = coment['message']
                    data_to_database['comment_created_time'] = coment['created_time']
                    self.database.insert('comment',data_to_database)
                    num_comments = num_comments +1
                    del data_to_database
        except:
            num_comments = 0
        
        return num_comments
