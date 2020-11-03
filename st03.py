import hashlib
import json
import os
import pprint
import requests

from datetime import datetime

CURRENT_DT = datetime.now()

ST03_TRACE_EVENT_ATTRIBUTES = \
    ['ACCOUNT', 'TERMINALID', 'nwHost', 'nwClient', 'nwUniqueId']

def _get_spans_from_st03_hitlist_resptime_event(event, traceid):
    spans = []
    # Create a root span
    _root_seed = '{}-{}-{}-{}-{}-{}'.format(str(CURRENT_DT), event['ACCOUNT'], event['TERMINALID'], event['ENDDATE'], event['ENDTIME'], 'ROOT')
    _root_span_id = hashlib.md5(_root_seed.encode('utf-8')).hexdigest()
    spans.append(
      {
          "id": _root_span_id,
          "trace.id": traceid,
          "attributes": {
            "service.instance.id": event['nwHost'],
            "service.name": "SAP-TOTAL-GUITIME",
            "span.kind": "SERVER",
            "duration.ms": event['GUITIME'] + event['GUINETTIME'], # event['RESPTI'],
            "name": "Complete",
            "customer.id": "{}@{}".format(event['ACCOUNT'], event['TERMINALID']),
            "description": "ST03_INFORWARDER:HITLIST_RESPTIME Job",
            "timestamp": event['timestamp']
          }
      }
    )

    return spans

def _get_trace_from_st03_hitlist_resptime_event(event):

    _seed = '{}-{}-{}-{}-{}'.format(str(CURRENT_DT), event['ACCOUNT'], event['TERMINALID'], event['ENDDATE'], event['ENDTIME'])
    _trace_id = hashlib.md5(_seed.encode('utf-8')).hexdigest()

    attributes_dict = {}
    for k in ST03_TRACE_EVENT_ATTRIBUTES:
        attributes_dict[k] = event[k]

    trace = \
    {
      "common": {
        "attributes": attributes_dict
      },
      "spans": _get_spans_from_st03_hitlist_resptime_event(event, _trace_id)
    }
    
    return trace

def _post_spans_to_trace_api(spans, account_id, insights_insert_key):
    pass




class NRInsightsQueryAPI:
  def __init__(self, insights_query_key, account_id):
    self.account_id = account_id
    self.headers = {
      'X-Query-Key': '{}'.format(insights_query_key)
      }
    self.query_url = 'https://insights-api.newrelic.com/v1/accounts/{}/query'.format(account_id)    
    
  def query(self, query):
    _qstring = "?nrql={}".format(query)
    return requests.get(self.query_url + _qstring, headers=self.headers)


class NRTraceInsertAPI:
  def __init__(self, insights_insert_key, account_id):
    self.account_id = account_id
    self.headers = {
      'content-type': 'application/json',
      'X-Insert-Key': '{}'.format(insights_insert_key)
      }
    self.insert_url = 'https://trace-api.newrelic.com/trace/v1'

  def insert(self, events):
    return requests.post(self.insert_url, data=json.dumps(events), headers=self.headers)

class NRLogInsertAPI:
  def __init__(self, insights_insert_key, account_id):
    self.account_id = account_id
    self.headers = {
      'content-type': 'application/json',
      'X-Insert-Key': '{}'.format(insights_insert_key)
      }
    self.insert_url = 'https://log-api.newrelic.com/log/v1'

  def insert(self, events):
    return requests.post(self.insert_url, data=json.dumps(events), headers=self.headers)

def post_st03_hitlist_resptime_traces(traces, account_id, insights_insert_key):
    _api = NRTraceInsertAPI(insights_insert_key, account_id)
    response = _api.insert(traces)
    if response.status_code != 202:
        raise Exception(
          "Error posting traces to trace API.  Response code: {}.  Error text: {}".format(
            response.status_code,
            response.text
            )
          )
    else:
        return response

def get_st03_hitlist_resptime_events(account_id, insights_query_key, time_lookback_minutes=5, fields_to_pull="*"):
    query_string = "FROM `ST03_INFORWARDER:HITLIST_RESPTIME` select {} SINCE {} minutes ago limit max".format(
        fields_to_pull,
        time_lookback_minutes,
      )
    _api = NRInsightsQueryAPI(account_id=account_id, insights_query_key=insights_query_key)
    response = _api.query(query_string)
    if response.status_code != 200:
        raise Exception(
          "Error getting ST03 data. Response code: {}.  Error text: {}".format(
                response.status_code,
                response.text)
            )    
    else:
        _json = response.json()
        if 'results' in _json:
            return _json['results'][0]['events']
        else:
            return []

def main():
    st03_recs = get_st03_hitlist_resptime_events(
      '2901317',
      os.getenv('NR_TRACEGEN_INSIGHTS_QUERY_KEY'),
      time_lookback_minutes=15,
      fields_to_pull= ",".join(
          ['ACCOUNT', 'TERMINALID', 'GUITIME', 'GUINETTIME', 'DBCALLS',
          'ENDDATE', '`ENDTIME`', 'ACCOUNT', 'TERMINALID', 'ENDDATE', 'nwUniqueId',
          'nwHost', 'nwClient', 'CPUTI', 'PROCTI', 'RESPTI',
          ]
         )

      )

    recs_with_gui_component = [rec for rec in st03_recs if rec['GUITIME'] or rec['GUINETTIME']]
    print("Number of recs with GUI component: {}".format(len(recs_with_gui_component)))

    trace_recs = []
    for rec in recs_with_gui_component:
        # print(rec['timestamp'], rec['ENDTIME'], rec['ENDDATE'])
        _trec = _get_trace_from_st03_hitlist_resptime_event(rec)
        trace_recs.append(_trec)
        rec['trace.id'] = _trec['spans'][0]['trace.id']
        rec['span.id'] = _trec['spans'][0]['id']
        rec['host'] = _trec['spans'][0]['attributes']['service.instance.id']
        rec['entity.name'] = _trec['spans'][0]['attributes']['service.name']
        rec['entity.type'] = 'SERVICE'
        rec['level'] = 'INFO'
        rec['message'] = "SAP_TOTAL_GUI_TIME={},ACCOUNT={},TERMINALID={},SAP_HOST={}".format(rec['GUITIME'] + rec['GUINETTIME'],rec['ACCOUNT'],rec['TERMINALID'],rec['nwHost'])
        rec['logtype'] = "SAP_TOTAL_GUI_TIME"
  
    trace_file = open('traces.json', 'w')
    trace_file.write(json.dumps(trace_recs, indent=4))
    trace_file.close()

    _log_api = NRLogInsertAPI(os.getenv('NR_TRACEGEN_INSIGHTS_INSERT_KEY'), '2901317')
    response = _log_api.insert(recs_with_gui_component)

    post_st03_hitlist_resptime_traces(trace_recs, '2901317', os.getenv('NR_TRACEGEN_INSIGHTS_INSERT_KEY'))


    
if __name__== "__main__": 
    main()
