import pandas as pd
import requests
import json
import os
import shutil

from datetime import datetime, timedelta
from googlesearch import search


class UpdateLinkPlanner:

    '''
    Updates linkbuilding sheet with the latest keywords ranking metrics and content written.
    '''

    def __init__(self, titles: list, key: str, period:int,limit:int,save_destination:str,
                 uploaded_content:str,link_building_planner:str):

        self.titles = titles
        self.key = key
        self.period = period
        self.limit = limit
        self.save_destination = save_destination
        self.uploaded_content = uploaded_content
        self.link_building_planner = link_building_planner

    def run(self):

        print('Fetching sites table...')

        sites_table = self.get_site_dataframe()

        print('Fetching keywords, country and languages table from Demandsphere...')

        master = self.get_key_place_lang(sites_table=sites_table)

        print('Fetching performance data from Demandsphere...')

        required = self.update_keyphrase_positions(master=master)

        print('Merging new file...')

        new = self.merge_with_planner(required=required)

        print('Checking if content was written previously...')

        content_written = self.content_written(new=new)

        print('Saving previous version...')

        self.save_previous()

        print('Rewriting with new version...')

        self.replace(new=content_written)

    def get_site_dataframe(self):

        titles = self.titles
        # Request site
        url = requests.get(
            'https://app.demandsphere.com/api/v3/account/sites?api_key={}'.format(self.key))

        # Get a list of all the sites
        sites = [site for site in url.json() if site['title'] in titles]

        # Transform into DataFrame
        sites_table = pd.DataFrame.from_records(sites)

        return sites_table

    def get_key_place_lang(self, sites_table: pd.DataFrame):

        # Set date_from and date_to for data being pulled
        dt = datetime.now() + timedelta(days=-2)

        day = dt.strftime("%Y-%m-%d")

        days_period_ago = (
            dt +
            timedelta(
                days=-
                self.period)).strftime("%Y-%m-%d")

        master = pd.DataFrame()

        # Iterate through all the websites and make a DataFrame of all the
        # keywords, their country and language

        for num in range(sites_table.shape[0]):

            site = sites_table.loc[num]

            params = {
                'api_key': self.key,
                'site_global_key': site['site_global_key'],
                'search_engine_global_key': site['search_engine_global_keys'][0],
                'date_from': days_period_ago,
                'date_to': day,
                'limit': 5000}

            url2 = requests.get(
                'https://app.demandsphere.com/api/v312/keywords/competitors_ranking',
                params=params)

            keywords = pd.DataFrame.from_records(
                url2.json()['rows']).reset_index(
                inplace=False)

            keywords['Country'] = sites_table['title'][num].split(" ")[0]

            keywords['Language'] = sites_table['title'][num].split(" ")[1]

            master = master.append(keywords, ignore_index=True, sort=True)

        return master

    def update_keyphrase_positions(self, master: pd.DataFrame):

        # set local and master rankings to 50 and update the ones that are <50
        # ranking
        master['master ff ranking'] = 50
        master['local ff ranking'] = 50

        # Iterate through nested ranking database to fetch page and ranking for local pages
        # Pages that don't rank will give a Value or Key error, I need to make error passing
        # more explicit in future versions
        for i, row in master.iterrows():
            try:
                master.at[i,
                          'local ff ranking'] = master.loc[i,
                                                           'rankings_data'][0]['rank']

            except ValueError:
                pass

            try:
                master.at[i, 'local ff page'] = master.loc[i,
                                                           'rankings_data'][0]['page']['url']

            except ValueError:
                master.at[i, 'local ff page'] = ""

            except KeyError:
                master.at[i, 'local ff page'] = ""

            # Iterate through nested ranking database to fetch page and ranking
            # for global pages
            try:
                master.at[i, 'master ff ranking'] = [page['rank'] for page in master.loc[i]['rankings_data']
                                                     if 'page' in page.keys() and page['page']['domain'] in ['www.farfetch.com']][0]
            except IndexError:
                pass

            except ValueError:
                pass

            try:
                master.at[i, 'master ff page'] = [page['page']['url'] for page in master.loc[i]['rankings_data']
                                                  if 'page' in page.keys() and page['page']['domain'] in ['www.farfetch.com']][0]
            except IndexError:
                pass

            required = master[['Country',
                               'Language',
                               'keyword_name',
                               'search_volume',
                               'local ff page',
                               'local ff ranking',
                               'master ff page',
                               'master ff ranking']].reset_index(inplace=False)

        # Top 10 boolean
        required['master_top_10'] = required['master ff ranking'] < 11

        # Replace '-' with 0 in search volume

        required['search_volume'] = required['search_volume'].replace('-', 0)

        return required

    def merge_with_planner(self, required: pd.DataFrame()):

        link_planner = pd.read_excel(self.link_building_planner)

        link_planner['key'] = link_planner['Country'] + \
            link_planner['Language'] + link_planner['keyword_name']

        required['key'] = required['Country'] + \
            required['Language'] + required['keyword_name']

        # required rows
        required_planner = link_planner[['key', 'recommended URL', 'Linked']]

        new = required.merge(required_planner, on='key', how='left')

        new = new.drop(columns=['key', 'index'])

        return new

    def content_written(self, new: pd.DataFrame()):

        all_keyphrases = pd.read_excel(self.uploaded_content)
        all_keyphrases['URLs'] = 'https://www.farfetch.com' + \
            all_keyphrases['URLs']

        new['content_written'] = new['recommended URL'].isin(
            all_keyphrases['URLs'])

        return new

    # def find_new_urls

    def save_previous(self):
        dt = datetime.now()
        dt = dt.strftime("%Y-%m-%d")

        # Copy new file to current directory
        dst_dir = self.save_destination
        shutil.copy(self.link_building_planner, dst_dir)
        new_name = dst_dir + 'linkbuilding_planner_{}.xlsx'.format(dt)
        if os.path.exists(new_name):
            raise Exception("Destination file exists!")
        else:
            os.rename(dst_dir + '/linkbuilding_planner.xlsx', new_name)

    def replace(self, new: pd.DataFrame):

        new = new.sort_values("search_volume", ascending=False)
        new.to_excel(self.link_building_planner, index=False)
