# Эксплуатация локального стенда

`make up` поднимает БД и брокер, выполняет миграцию и seed, запускает API, worker и диспетчер. Успешный `migrate` остаётся завершённым контейнером — это нормально. Повторный seed не сбрасывает историю и счётчики клиентов.

## Задача долго остаётся pending

1. Посмотреть `/admin/queue` с `X-Admin-Token`: количество pending/failed, неопубликованные сообщения и heartbeat.
2. Проверить `docker compose ps` и `docker compose logs --tail=100 worker dispatcher rabbitmq`.
3. Если брокер остановлен, выполнить `docker compose start rabbitmq`. Диспетчер отправит сохранённые задачи.
4. Если worker упал, выполнить `docker compose up -d worker`. Брокер повторно доставит неподтверждённую задачу, а outbox повторит pending-задачи.

Нельзя «чинить» задачу удалением записи журнала или ручной сменой итоговой суммы счёта. При неверном расходе нужно добавить исправление через API.

## Задача failed

Логи worker содержат техническую причину. API возвращает краткую ошибку, чтобы не показывать внутренние данные клиенту. После устранения причины:

```bash
curl -X POST http://localhost:8210/admin/jobs/JOB_UUID/retry \
  -H 'X-Admin-Token: local-demo-meterledger-admin-change-before-deployment'
```

Повторный запуск использует актуальный журнал на момент выполнения. Счёт, который уже был успешно выпущен, не меняется. Счётчик попыток сбрасывается; отдельной долговременной истории административных повторов пока нет.

## Данные и остановка

`make down` удаляет контейнеры и сеть, сохраняя volumes. Для обычной паузы достаточно `docker compose stop`. Команда `docker compose down -v` удаляет данные стенда; в CI она используется только для временного окружения runner.

`make test` использует отдельную PostgreSQL в tmpfs и не обращается к рабочим таблицам. После тестов её можно остановить: `docker compose --profile test stop test-db`.

## Проверка восстановления

`make recovery` требует Docker CLI, Python 3.12 и `uv sync --extra dev --frozen`. Скрипт работает только с Compose-проектом этой папки. Он создаёт отдельного клиента для каждого сценария, останавливает брокер, завершает worker/dispatcher через SIGKILL и проверяет результат. Рабочие данные не удаляются, демонстрационные записи остаются для просмотра.

В конце скрипт запускает брокер и возвращает задержки `WORKER_BEFORE_COMMIT_DELAY` и `DISPATCHER_AFTER_PUBLISH_DELAY` к нулю. Если процесс скрипта завершён извне, восстановить настройки можно так:

```bash
WORKER_BEFORE_COMMIT_DELAY=0 DISPATCHER_AFTER_PUBLISH_DELAY=0 \
  docker compose up -d --no-deps --force-recreate worker dispatcher
docker compose start rabbitmq
```
