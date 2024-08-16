# plugins/steps/messages.py
from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными

    hook = TelegramHook(telegram_conn_id='test',
                        token='{7259779851:AAGfiBIMJFp9kWutgXgFdf6Ag6zsCBra8jw}',
                        chat_id='{4225233363}')
    dag = context['dag']
    run_id = context['run_id']
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '{4225233363}',
        'text': message
    }) # отправление сообщения правление сообщения

def send_telegram_failure_message(context):
    hook = TelegramHook(telegram_conn_id='test',
                        token='{7259779851:AAGfiBIMJFp9kWutgXgFdf6Ag6zsCBra8jw}',
                        chat_id='{4225233363}')
    dag = context['dag']
    run_id = context['run_id']
    str_key = context['task_instance_key_str']
    
    message = f'Исполнение DAG {dag} с id={run_id} и task_instance_key_str={str_key} прошло неудачно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '{4225233363}',
        'text': message
    }) # отправление сообщения 

