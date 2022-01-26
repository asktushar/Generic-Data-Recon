package com.asktushar.playground.conf

case class ArgConf(
                    taskType: String,
                    master: String,
                    complaintsSourcePath: String,
                    categorySourcePath: String,
                    serviceSourcePath: String,
                    complaintsReportPath: String)
