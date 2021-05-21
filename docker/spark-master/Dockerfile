FROM spydernaz/spark-base:latest
COPY --from=saasglue/agent:v1.0 / /

COPY start-master.sh /

ADD sg-agent-launcher /
ADD sg.cfg /

ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_LOG /spark/logs

EXPOSE 8080 7077 6066

CMD ["/bin/bash", "/start-master.sh"]
