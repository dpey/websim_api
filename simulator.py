import requests
from requests import Session
import requests.utils
import urllib
import pandas as pd
import numpy as np
import getpass
import time
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib
import matplotlib.ticker as ticker
import json
import pickle as pkl
from datetime import datetime
import sys
import os.path
from multiprocessing import Pool
import re
import itertools


def unwrap_simulate(arg, **kwarg):
    return simulator.simulate(*arg, **kwarg)


def unwrap_grab_statistics(arg, **kwarg):
    return simulator.grab_statistics(*arg, **kwarg)


class simulator:

    def __init__(self):
        '''Init method'''
        self.dir = ''
        self.dir_log = ''
        self.dir_data = ''
        self.pswd_ws = ''
        self.alphas_list = []
        self.ssn = 0
        self.alias = 'AG49601'
        self.df_input = pd.DataFrame(columns=[
                                     'code', 'opcodetype', 'region', 'univid', 'opneut', 'optrunc', 'decay', 'parent'])
        self.results_keys = ['alpha_id', 'sim_date', 'submit_date', 'code', 'opcodetype',
                             'region', 'univid', 'opneut', 'optrunc', 'decay', 'fit_step',
                             'sharpe', 'fitness', 'turnover', 'long', 'short', 'weight_error',
                             'subuniv_warning', 'prod_corr', 'self_corr', 'parent',
                                                'to_submit', 'submition_status']
        self.df_results = pd.DataFrame(columns=self.results_keys)
        self.df_archive = pd.DataFrame(
            columns=['alpha_id', 'statistics', 'PnL', 'Sharpe'])
        self.max_time_sim = 10*60
        self.max_time_self_corr = 3*60
        self.max_time_prod_corr = 3*60
        self.max_time_warning = 3*60
        self.current_time = 0
        self.color = {'red': 'fabcaa', 'yellow': 'fdf297', 'green': 'b5dbad',
                      'blue': 'acc3e2', 'magenta': 'c4b6d6', 'None': ''}
        self.debug = False
        self.max_tabs = 2
        self.log_error = []
        self.df_results_file = ''
        self.df_archive_file = ''

    def debug_on(self):
        self.debug = True

    def debug_off(self):
        self.debug = False

    def get_df_results(self):
        return self.df_results

    def load_df_results(self, file):
        self.df_results = pd.read_csv(file)
        self.df_results_file = file

    def save_df_results(self, *args):
        if len(args) > 0:
            self.df_results.to_csv(self.dir_data + args[0])
            print('[SAVED] df_results was saved to /data/' + args[0])
        else:
            self.df_results.to_csv(self.dir_data + self.df_results_file)
            print('[SAVED] df_results was saved to /data/' + self.df_results_file)

    def load_df_archive(self, file):
        self.df_archive = pd.read_csv(file)
        self.df_archive_file = file

    def get_df_archive(self):
        return self.df_archive

    def save_df_archive(self, *args):
        if len(args) > 0:
            self.df_archive.to_csv(self.dir_data + args[0])
            print('[SAVED] df_archive was saved to /data/' + args[0])
        else:
            self.df_archive.to_csv(self.dir_data + self.df_archive_file)
            print('[SAVED] df_archive was saved to ' + self.df_archive_file)

    def read_password(self):
        '''Read password method'''
        with open('lib.pkl', 'rb') as pswd:
            return pkl.load(pswd)

    def save_session(self):
        with open('session.pkl', 'wb') as f:
            pkl.dump(self.ssn, f)

    def get_session(self):
        with open('session.pkl', 'rb') as f:
            cookies = pkl.load(f)
        return cookies

    def load_session(self):
        self.ssn = self.get_session()

    def log_in(self):
        '''Log in method'''
        self.pswd_ws = self.read_password()
        # Login requests
        user_credentials_d = {
            'EmailAddress': 'name@domain.com', 'Password': self.pswd_ws}
        login_req_header_d = {'Content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                              'Referer': 'https://websim.worldquantchallenge.com/login'}

        # Create new session
        self.ssn = requests.Session()

        for i in range(3):

            # Log in to WebSim
            login_req = self.ssn.post('https://websim.worldquantchallenge.com/login/process',
                                      data=user_credentials_d,
                                      headers=login_req_header_d,
                                      verify=False)
            # Check log in
            login_check = self.ssn.post(
                'https://websim.worldquantchallenge.com/user')
            try:
                if (login_check.json()['result']['UserAlias'] == self.alias):
                    print('[LOG IN]Log in complete')
                    self.save_session()
                    return
                else:
                    print('Error - unexpectable user. Log in failed')
                    return
            except:
                print('[ERROR]Log in failed')
                print(str(3-i) + ' tries left')
                return
            timer.sleep(10)
        return

    def log_in_check(self, session):
        login_check = session.post(
            'https://websim.worldquantchallenge.com/user')
        try:
            if (login_check.json()['result']['UserAlias'] == 'AA00001'):
                print('[LOG IN] Login check complete')
            else:
                print('[ERROR] Login check failed')
        except:
            print('[ERROR} Login check failed')

    def save_log(self):
        path = self.dir_log + \
            str(datetime.now().strftime("%Y-%m-%d_%H:%M"))+'.json'
        with open(path, 'wb') as f:
            json.dump({'alphas': self.log_error}, f)
        print('[SAVED} Error log was saved, ' +
              str(len(self.log_error)) + ' alpha(s) in ')

    def load_df_input(self, alpha):
        self.df_input = self.df_input.append(alpha, ignore_index=True)
        self.df_input.drop_duplicates(inplace=True)

    def get_df_input(self):
        return self.df_input

    def clear_df_input(self):
        self.df_input.drop(self.df_input.index, inplace=True)

    def create_simulate_data(self, alpha):
        default_simulate = {"delay": "1",
                            "unitcheck": "off",
                            "univid": "100",
                            "opcodetype": "code,
                            "opassetclass": "stocks",
                            "optrunc": "1",
                            "code": "1",
                            "region": "ALL",
                            "opneut": "none",
                            "IntradayType": 0,
                            "tags": "alpha",
                            "decay": 0,
                            "DataViz": "1",
                            "backdays": 104,
                            "simtime": "Y3"}
        for key in default_simulate.iterkeys():
            if key in alpha:
                default_simulate[key] = alpha[key]

        string = str([default_simulate]).replace("'", '"').replace('u"', '"')
        return {'args': string}

    def print_pnl(self, alpha_id):
        if not os.path.isfile(self.dir+'data/pnl/'+str(alpha_id)+'.csv'):
            flag = self.download_pnl(alpha_id)
            if flag != 0:
                return 0
        try:
            pnl_df = pd.read_csv(self.dir + 'data/pnl/'+str(alpha_id) + '.csv')
        except:
            print('[ERROR] Cant find Pnl file /data/pnl' +
                  str(alpha_id) + '.csv')
            return
        pnl_df['date'] = pnl_df['date'].apply(
            lambda x: datetime.strptime(x, '%Y-%m-%d'))
        fig, ax = plt.subplots(1, 1)
        ax.plot(pnl_df['date'], pnl_df['pnl'].cumsum()/10e+5)
        ax.xaxis.set_major_locator(ticker.MultipleLocator(360))
        ax.set_xlabel('time')
        ax.set_ylabel('PnL, mln $')
        plt.show()
        return

    def download_pnl(self, alpha_id, n_try=3):
        pnl_data = {'args': '{"alpha_list":["' + str(alpha_id) + '"]}'}
        for i in range(n_try):
            try:
                request_pnl = self.ssn.post(
                    'https://websim.worldquantchallenge.com/alphas/pnlchart/generate', data=pnl_data)
                time.sleep(3)
                request_id = request_pnl.json()['result']['RequestId']
                response_pnl = self.ssn.post(
                    'https://websim.worldquantchallenge.com/alphas/pnlchart/result/' + str(request_id))
                time.sleep(1)
                response_pnl_result = response_pnl.json()['result']
                pnl_dict = json.loads(
                    response_pnl_result[u'response'])[alpha_id]
                pnl_df = pd.DataFrame(
                    columns=['date', 'pnl', 'sharpe'], data=pnl_dict[u'pnlchart'])
                pnl_df['date'] = pnl_df['date'].apply(
                    lambda x: datetime.strptime(x, '%Y%m%d'))
                pnl_df['pnl'] = pnl_df['pnl'].apply(lambda x: float(x))
                pnl_df['sharpe'] = pnl_df['sharpe'].apply(lambda x: float(x))
                pnl_df.to_csv(self.dir + 'data/pnl/' +
                              str(alpha_id) + '.csv', index=False)
                if self.debug:
                    print(
                        '[SAVED] PnL and Sharpe were saved, alpha_id: ' + str(alpha_id))
                return 0
            except:
                time.sleep(3)
        print('[ERROR] Cant load Pnl, alpha_id: ' + str(alpha_id))
        return -1

    def download_statistics(self, alpha_id, n_try=3):
        statistics_data = {'args': '{"alpha_list":["' + str(alpha_id) + '"]}'}
        for i in range(n_try):
            try:
                sim_stats = self.ssn.post(
                    'https://websim.worldquantchallenge.com/alphainfo', data=statistics_data)
                time.sleep(1)
                sim_stats_table = sim_stats.json(
                )['result']['alphaInfo']['AlphaSimSum']
                stats_df = pd.DataFrame(sim_stats_table)
                stats_df.to_csv(self.dir + 'data/stats/' +
                                str(alpha_id) + '.csv', index=False)
                if self.debug:
                    print('[SAVED] Statistics was saved, alpha_id: ' + str(alpha_id))
                return 0
            except:
                time.sleep(3)
        print('[ERROR] Cant load statistics, alpha_id: ' + str(alpha_id))
        return -1

    def get_statistics(self, alpha_id):
        if not os.path.isfile(self.dir + 'data/stats/' + str(alpha_id) + '.csv'):
            flag = self.download_statistics(alpha_id)
            if flag != 0:
                return0
        try:
            stats_df = pd.read_csv(
                self.dir + 'data/stats/' + str(alpha_id) + '.csv')
        except:
            print('[ERROR] Cant find statistics file /stats/pnl/' +
                  str(alpha_id) + '.csv')
            return 0
        return stats_df

    def get_self_correlation(self, alpha_id, n_try=3):
        correlation_self_data = {
            'args': '{"alpha_list":["' + str(alpha_id) + '"], "corr_type":"self_corr"}'}
        for i in range(n_try):
            sim_stats_self_corr = self.ssn.post(
                'https://websim.worldquantchallenge.com/correlation/start', data=correlation_self_data)
            time.sleep(1)
            try:
                sim_request_id = str(sim_stats_self_corr.json()[
                                     'result'].values()[0])
            except:
                print(
                    '[ERROR] Self correlation could not be loaded for alpha_id: ' + str(alpha_id))
                return 0
            FLAG = 0
            ERROR = 0
            while (FLAG < self.max_time_self_corr):
                if self.debug:
                    print('Correlation loading')
                FLAG = FLAG + 10
                time.sleep(10)
                sim_stats_self_corr_progress = self.ssn.post(
                    'https://websim.worldquantchallenge.com/correlation/result/' + sim_request_id)
                if (~sim_stats_self_corr_progress.json()['result']['InProgress']):
                    FLAG = self.max_time_self_corr
            try:
                sim_stats_self_corr_json = json.loads(
                    sim_stats_self_corr_progress.json()['result']['response'])
                sim_stats_self_corr_max = sim_stats_self_corr_json[str(
                    alpha_id)]['Result']['TopBracket'][0]['Correlation']
                if self.debug:
                    print('Max self correlation is', sim_stats_self_corr_max)
                return sim_stats_self_corr_max
            except:
                time.sleep(3)
        print('[ERROR] Self correlation unavailable')
        return 0

    def get_prod_correlation(self, alpha_id):
        correlation_prod_data = {
            'args': '{"alpha_list":["' + str(alpha_id) + '"], "corr_type":"prod_corr"}'}
        for i in range(n_try):
            sim_stats_prod_corr = self.ssn.post(
                'https://websim.worldquantchallenge.com/correlation/start', data=correlation_prod_data)
            time.sleep(1)
            try:
                sim_request_id = str(sim_stats_prod_corr.json()[
                                     'result'].values()[0])
            except:
                print(
                    '[ERROR] Prod correlation could not be loaded for alpha_id: ' + str(alpha_id))
                return 0
            FLAG = 0
            ERROR = 0
            while (FLAG < self.max_time_prod_corr):
                if self.debug:
                    print('Correlation loading')
                FLAG = FLAG + 10
                time.sleep(10)
                sim_stats_prod_corr_progress = self.ssn.post(
                    'https://websim.worldquantchallenge.com/correlation/result/' + sim_request_id)
                if (~sim_stats_prod_corr_progress.json()['result']['InProgress']):
                    FLAG = self.max_time_self_corr
            try:
                sim_stats_prod_corr_json = json.loads(
                    sim_stats_prod_corr_progress.json()['result']['response'])
                sim_stats_prod_corr = sim_stats_prod_corr_json[str(
                    alpha_id)]['Result']
                sim_stats_prod_reshape = [list(x)
                                          for x in zip(*sim_stats_prod_corr)]
                index = 0
                for i in range(len(sim_stats_prod_reshape[2])):
                    if (sim_stats_prod_reshape[2][i] > 0):
                        index = i
                prod_corr = float(sim_stats_prod_reshape[1][index])
                if self.debug:
                    print('Max prod correlation is '+str(prod_corr))
                return prod_corr
            except:
                time.sleep(3)
        print('[ERROR] Prod correlation unavailable')

    def check_submission(self, alpha_id, n_try=3):
        check_submission_data = {
            'args': '{"alpha_list":["' + str(alpha_id) + '"]}'}
        for i in rang(n_try):
            #
            sim_check_submission = self.ssn.post(
                'https://websim.worldquantchallenge.com/submission/check', data=check_submission_data)
            time.sleep(1)
            try:
                check_request_id = str(sim_check_submission.json()[
                                       'result']['RequestId'])
            except:
                print('[ERROR] Check Submission request error')
                return 0
            FLAG = 0
            ERROR = 0
            while (FLAG < self.max_time_warning):
                if self.debug:
                    print('Submission checking')
                FLAG = FLAG + 15
                time.sleep(7)
                sim_check_submission_progress = self.ssn.post(
                    'https://websim.worldquantchallenge.com/submission/result' + check_request_id)
                time.sleep(8)
                if (sim_check_submission_progress.json()['error'] > 0):
                    if self.debug:
                        print(sim_check_submission_progress.json())
                    print('[ERROR] ' +
                          sim_check_submission_progress.json()['error'])
                    return sim_check_submission_progress.json()['error']
                if (sim_check_submission_progress.json()['result'] > 0):
                    if (sim_check_submission_progress.json()['result']['InProgress'] != True):
                        if self.debug:
                            print(sim_check_submission_progress.json())
                        return sim_check_submission_progress.json()['result']['response']
        print('[ERROR] Check submission failed')
        return 0

    def create_metadata_data(alpha_id="", category=0, name=0, tags=[], color='None', favorite=0,
                             hidden=0, isInOS=0, desc=0):
        if len(alphaId) < 1:
            print('Incorrect alphaId')
            return -1
        metadata = {'category': category,
                    'name': name,
                    'tags': tags,
                    'color': self.color[color],
                    'favorite': favorite,
                    'hidden': hidden,
                    'isInOS': isInOS,
                    'desc': desc,
                    'alpha_id': alphaId.encode('ascii')}
        string = str(metadata).replace("'", '"')
        return {'args': string}

    def add_metadata(self, name='anonymous', color='None', alpha_id='0'):
        metadata_data = create_metadata_data(
            name=name, color=color, alphaId=alpha_id)
        metadata_submit = self.ssn.post(
            'https://websim.worldquantchallenge.com/alphameta', data=metadata_data)
        return

    def simulate(self, alpha):
        simulate_data = self.create_simulate_data(alpha)
        ssn_local = self.get_session()
        if self.debug:
            self.log_in_check(ssn_local)

        counter = 0
        while (counter < 2):
            counter = counter+1
            simtest = ssn_local.post('https://websim.worldquantchallenge.com/simulate',
                                     data=simulate_data)
            # time.sleep(1)
            try:
                sim_id = str(simtest.json()['result'][0])
                if self.debug:
                    print('Sim id:' + sim_id)
                if (sim_id == 'None'):
                    counter = counter + 1
                    continue
            except:
                time.sleep(4)
                counter = counter + 1
                if self.debug:
                    print('Simulate request fail')
                continue
            if self.debug:
                print('Simulation start')
            FLAG = 0
            while (FLAG < self.max_time_sim):
                FLAG = FLAG + 30
                time.sleep(15)
                sim_progress = ssn_local.post(
                    'https://websim.worldquantchallenge.com/job/progress/' + sim_id)
                try:
                    if self.debug:
                        print(sim_progress.json())
                    time.sleep(15)
                    if (sim_progress.json() == 'DONE'):
                        if self.debug:
                            print('Simulation done')
                        return sim_id

                    if (sim_progress.json() == 'ERROR'):
                        counter = counter + 1
                        if self.debug:
                            print('Simulation error')
                        break
                except:
                    print('[ERROR] Simulation progress checking error')

            if (FLAG >= self.max_time_sim):
                counter = 3

        if self.debug:
            print('Simulation failed. "0" will be returned')
        return "0"

    def grab_statistics(self, alpha):
        alpha_id = alpha['alpha_id']
        if (alpha_id == '0'):
            self.log_error.append(batch_list[j])
            return 0
        alpha_dict = dict.fromkeys(self.results_keys)
        alpha_dict['alpha_id'] = alpha_id
        for key in alpha_dict.iterkeys():
            if key in alpha:
                alpha_dict[key] = alpha[key]
        alpha_dict['sim_date'] = datetime.datetime.now().date()
        alpha_stats = self.get_statistics(alpha_id)
        alpha_pnl_stats = alpha_pnl.download_pnl(alpha_id)
        alpha_self_corr = self.get_self_correlation(alpha_id)
        alpha_prod_corr = self.get_prod_correlation(alpha_id)
        alpha_check_submit = self.check_submission(alpha_id)

    def multi_research(self, batch_size=10):
        self.current_time = datetime.now()
        df_input_buff = self.df_input.copy()
        p = Pool(self.max_tabs)
        for i in range(1+int(np.floor(len(df_input_buff)/batch_size))):

            batch_list = []
            if (len(df_input_buff[i*batch_size:]) > batch_size):
                batch_list = [df_input_buff.iloc[i].to_dict()
                              for i in range(i*batch_size, (i+1)*batch_size)]
            else:
                batch_list = [df_input_buff.iloc[i].to_dict()
                              for i in range(i*batch_size, len(df_input_buff))]
            if len(batch_list) < 0:
                break
            for j in range(len(batch_list)):
                batch_list[j]['decay'] = int(batch_list[j]['decay'])

            batch_sim_id = p.map(unwrap_simulate, zip(
                [self]*len(batch_list), batch_list))
            #batch_sim_id = p.map(unwrap_simulate, (batch_list))

            dt = datetime.now() - self.current_time
            if (dt.total_seconds() > 10800.0):
                print('Re-login')
                self.current_time = datetime.now()
                self.log_in()

            for j in range(len(batch_sim_id)):
                if batch_sim_id[j] == "0":
                    self.log_error.append(batch_list[j])

        with open('log_error.pkl', 'wb') as f:
            pkl.dump(self.log_error, f)

        return
