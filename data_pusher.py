#!/usr/bin/python
# -*- coding: utf-8 -*-
from asyncio import sleep
import json
from pprint import pprint
from wsgiref import headers
from clickhouse_driver import connect
from clickhouse_driver.errors import Error
from sql.clickhouse_query import ClickhouseQuery
from server.clickhouse import ClickhouseServer
import os
import requests
from datetime import datetime, timedelta
import argparse



def pull_datas_from_grafana(url, cookie, resource_type,date_to = datetime.utcnow(),date_from = datetime.utcnow()- timedelta(minutes=15)):
    mapping_resource_type = {
        "Memory": os.environ["PAYLOAD_MEMORY"],
        "CPU": os.environ["PAYLOAD_CPU"],
    }

    
    date_to_str = date_to.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    date_from_str = date_from.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    with open(mapping_resource_type[resource_type], "r") as payload_cpu:
        payload_cpu = json.loads(payload_cpu.read())

    payload_cpu["range"]["from"] = date_from_str
    payload_cpu["range"]["to"] = date_to_str

    cpu_from_timestamp = "{}{}".format(date_from.strftime("%s"), "000")

    cpu_to_timestamp = "{}{}".format(date_to.strftime("%s"), "000")

    payload_cpu["from"] = cpu_from_timestamp
    payload_cpu["to"] = cpu_to_timestamp

    data = requests.post(
        url=url,
        headers={
            "cookie": cookie,
        },
        json=payload_cpu,
    )

    result_dict = json.loads(data.text)

    full_label_datas = {}

    for data_in_frame in result_dict["results"]["B"]["frames"]:
        labels = data_in_frame["schema"]["fields"][1]["labels"]
        full_label_datas[labels["pod"]] = labels

    for data_in_frame in result_dict["results"]["A"]["frames"]:
        data_value = data_in_frame["data"]["values"][1][0]
        if data_value > 80:
            pod = data_in_frame["schema"]["fields"][1]["labels"]["pod"]
            container = full_label_datas[pod]["label_app"]
            team = full_label_datas[pod]["label_autonomic_ai_team"]
            final_datas = (date_to_str, team, pod, container, resource_type)
            yield final_datas

if __name__ == "__main__":
    """
    2022-08-19T13:32:43.611522Z
    2022-08-19T13:17:43.611525Z
    """
    parser = argparse.ArgumentParser()

    # Adding optional argument
    parser.add_argument("-f", "--DateFrom", help = "Date From (2022-08-19T13:17:43.611522Z)",default="2022-08-19T13:17:43.611522Z")
    parser.add_argument("-t", "--DateTo", help = "Date To (2022-08-19T13:32:43.611522Z)",default="2022-08-19T13:32:43.611522Z")

    
    # Read arguments from command line
    args = parser.parse_args()

    grafana_cookie = os.environ["GRAFANA_COOKIE"]
    grafana_url = os.environ["GRAFANA_URL"]

    datas_dict = {}

    for resource in ["CPU","Memory"]:
        datas_dict[resource] = pull_datas_from_grafana(
            grafana_url,
            grafana_cookie,
            resource,
            date_from=datetime.strptime(args.DateFrom, "%Y-%m-%dT%H:%M:%S.%fZ"),
            date_to=datetime.strptime(args.DateTo, "%Y-%m-%dT%H:%M:%S.%fZ")
        )

    columns = {
            "time_get_data":"String",
            "team":"String",
            "pod":"String",
            "container":"String",
            "resource":"String",
    }

    clickhouse = ClickhouseServer()
    clickhouse.init()
    
    list_columns = ",".join(["{} {}".format(column,columns[column]) for column in columns.keys()])
    clickhouse.create_table_with_columns("tb_resources_monitoring",list_columns,"default","container")
    
    column_for_insert = "({})".format(",".join([column for column in columns.keys()]))

    for data in datas_dict.values():
        clickhouse.insert_data("default","tb_resources_monitoring",column_for_insert,data)
