import json
import os
import sys
import requests
import certifi

from .exceptions import *

from elasticsearch6 import Elasticsearch


class Ivis:
    """Helper class for ivis tasks"""

    # to preserve the API, this field must be static (store_state is static, but requires the url) 
    _request_url_base = ""

    def __init__(self):
        self._data = json.loads(sys.stdin.readline())
        esUrl = str(self._data['es']['host']) + ":" + str(self._data['es']['port'])
        ca_path = self._data['caPath']
        # a flag inidicating whether to inject the CA certificate or not 
        set_ca = False
        Ivis._request_url_base = self._data['server']['trustedUrlBase']
        # this might result (>1 parallel runs) in more tasks writing the same CA into the certifi file 
        try:
            # try to resolve IVIS
            requests.get(Ivis._request_url_base)
        except requests.exceptions.SSLError as err:
            # on SSL failure, inject certificate
            # this should happen only once
            # when the CA is injected, subsequent requests should succeed
            set_ca = True
            ca_cert_str = ""
            with open(ca_path, 'r') as ca_file:
                ca_cert_str = ca_file.read()
            
            cafile = certifi.where()
            with open(cafile, 'a') as outfile:
                outfile.write('\n')
                outfile.write(ca_cert_str)

        if self._data['certs']:
            cert_path = self._data['certPath']
            key_path = self._data['keyPath']
            # CA injection needed also for the elasticsearch client
            if set_ca:
                self._elasticsearch = Elasticsearch(esUrl, use_ssl=True, ca_certs=ca_path, client_cert=cert_path, client_key=key_path, verify_certs=True)
            else:
                self._elasticsearch = Elasticsearch(esUrl, use_ssl=True, client_cert=cert_path, client_key=key_path, verify_certs=True)
        else:
            self._elasticsearch = Elasticsearch([esUrl])
        self.state = self._data.get('state')
        self.params = self._data['params']
        self.entities = self._data['entities']
        self.owned = self._data['owned']
        self._accessToken = self._data['accessToken']
        self._jobId = self._data['context']['jobId']
        self._sandboxUrlBase = self._data['server']['sandboxUrlBase']

    @property
    def elasticsearch(self):
        return self._elasticsearch

    @staticmethod
    def _request(msg):
        url = Ivis._request_url()
        response = dict()
        try:
            requests.post(url, data=json.dumps(msg)).json()
        except requests.ConnectionError :
            raise RequestException('Could not estabilish connection to the IVIS-core instance (address: ' + url + ')')
        error = response.get('error')
        if error:
            raise RequestException(error)
        return response
    
    @staticmethod
    def _request_url():
        return Ivis._request_url_base + "/rest/remote/emit" # TODO

    def create_signals(self, signal_sets=None, signals=None):
        msg = {
            'type': 'create_signals',
        }

        if signal_sets is not None:
            msg['signalSets'] = signal_sets

        if signals is not None:
            msg['signals'] = signals

        response = Ivis._request(msg)

        # Add newly created to owned
        for sig_set_cid, set_props in response.items():
            signals_created = set_props.get('signals', {})
            if signal_sets is not None:
                # Function allows passing in either array of signal sets or one signal set
                if (isinstance(signal_sets, list) and any(map(lambda s: s["cid"] == sig_set_cid, signal_sets))) or (
                        not isinstance(signal_sets, list) and signal_sets["cid"] == sig_set_cid):
                    self.owned.setdefault('signalSets', {}).setdefault(sig_set_cid, {})
            setEntity = dict(set_props)
            setEntity.pop('signals', None) # Don't belong to entities
            self.entities['signalSets'].setdefault(sig_set_cid, setEntity)
            if signals_created:
                self.owned.setdefault('signals', {}).setdefault(sig_set_cid, {})
                for sigCid, sig_props in signals_created.items():
                    self.owned['signals'][sig_set_cid].setdefault(sigCid, {})
                    self.entities['signals'].setdefault(sig_set_cid, {}).setdefault(sigCid, sig_props)

        return response

    def create_signal_set(self, cid, namespace, name=None, description=None, record_id_template=None, signals=None):

        signal_set = {
            "cid": cid,
            "namespace": namespace
        }

        if name is not None:
            signal_set["name"] = name
        if description is not None:
            signal_set["description"] = description
        if record_id_template is not None:
            signal_set["record_id_template"] = record_id_template
        if signals is not None:
            signal_set['signals'] = signals

        return self.create_signals(signal_sets=signal_set)

    def create_signal(self, signal_set_cid, cid, namespace, type, name=None, description=None, indexed=None,
                      settings=None,
                      weight_list=None, weight_edit=None, **extra_keys):

        # built-in type is shadowed here because this way we are able to call create_signal(set_cid, **signal),
        # where signal is dictionary with same structure as json that is accepted by REST API for signal creation

        signal = {
            "cid": cid,
            "type": type,
            "namespace": namespace,
        }

        if indexed is not None:
            signal["indexed"] = indexed
        if settings is not None:
            signal["settings"] = settings
        if weight_list is not None:
            signal["weight_list"] = weight_list
        if weight_edit is not None:
            signal["weight_edit"] = weight_edit
        if name is not None:
            signal["name"] = name
        if description is not None:
            signal["description"] = description

        signal.update(extra_keys)

        signals = {signal_set_cid: signal}

        return self.create_signals(signals=signals)

    @staticmethod
    def store_state(state):
        msg = {
            "type": "store_state",
            "state": state
        }
        return Ivis._request(msg)

    def upload_file(self, file):
        url = f"{self._sandboxUrlBase}/{self._accessToken}/rest/files/job/file/{self._jobId}/"
        response = requests.post(url, files = {"files[]": file})

    def get_job_file(self, id):
        return requests.get(f"{self._sandboxUrlBase}/{self._accessToken}/rest/files/job/file/{id}")


ivis = Ivis()