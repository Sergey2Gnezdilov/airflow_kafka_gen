import requests


def send_messages(message, token, chat_id, max_rows=50):
    msg_lst = message.splitlines()
    if len(msg_lst) <= max_rows:
        send_message(message, token, chat_id)
    else:
        j = 0
        for i in range(0, len(msg_lst), max_rows):
            if i == 0:
                continue
            send_message("\n".join(msg_lst[j:i]), token, chat_id)
            j = i
        send_message("\n".join(msg_lst[j:]), token, chat_id)


def send_message(message, token, chat_id):
    response = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
    )
    response.raise_for_status()


def failed_task_notification(kwargs):
    from airflow.models import Variable

    log_url = kwargs["ti"].log_url.replace("localhost:8080", "airflow.yourdomain.com")
    exception = "None"
    if kwargs.get("exception"):
        exception = kwargs["exception"]
    message = (
        f"<a href='{log_url}'>ErrorLog</a>"
        f"\nDAG:{kwargs['ti'].dag_id}"
        f"\nRunId:{kwargs['run_id']}"
        f"\nTask:{kwargs['ti'].task_id} (try {kwargs['ti'].try_number - 1}/{kwargs['ti'].max_tries + 1})"
        f"\nDate:{kwargs['logical_date']}"
        f"\n{exception}"
    )

    channel = Variable.get("you_alerts", deserialize_json=True)
    send_message(message, channel.get("token"), channel.get("chat_id"))
