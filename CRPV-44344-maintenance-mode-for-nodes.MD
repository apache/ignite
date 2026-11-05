# CRPV-44344 Maintenance mode для части кластера

* Задача
* Условия и ограничения
* Описание реализации "Режим обслуживания кластера"
* Ссылки

## Задача

### Режим обслуживания ЧАСТИ КЛАСТЕРА

## Условия и ограничения

* Версия 19.0
* Настроен плагин безопасности.
* Кластер находится в режиме обслуживания `state = MAINTENANCE`.

### RU 

В процессе обновления переход в режим maintenance отключен.
Работает "по-старому" - все ноды находятся в активном режиме.

## Описание реализации 

### Описание работы ноды в режиме обслуживания

1. Ноды переходят в режим обслуживания с помощью специальной команды.
2. В режиме обслуживания:
    * На нодах не выполняются никакие пользовательские команды, запросы и процессы. 
        * Вызов cache API, thin client API, SQL, Compute, Service завершается ошибкой. 
        * Нода исключается из методов пользовательского API, которые возвращают состав кластера.
        * Процесс снятия Снапшота/дампа НЕ выполняется.
        * (?) В том числе не выполняется обновление данных если нода хранит резервную копию некоторой партиции.
        * (?) На ноде не выполняется создание и удаление кешей.
    * На нодах запрещено создание новых клиентских подключений (Thin, Client Node, ODBC, JDBC).
    * Существующие соединения НЕ закрываются.
3. После перехода ноды(нод) в "обычный" режим происходит процесс ребаланса.

### Выполнение работ на нодах в режиме обслуживания

1. Администратор переводит ноды в режим обслуживания.
2. После перевода клиенты переключаются на оставшиеся ноды (процесс переключения реализуется внешними средствами). 
3. Выполняются работы.
4. По окончании работ ноды переводятся в обычный режим.
5. Происходит ребаланс.

### Команды

```shell
# Перевод нод в режим обслуживания.
> ./control.sh --maintenance-nodes --put consitentId1,consistentId2...consistentIdN

# Перевод нод в обычный обслуживания.
> ./control.sh --maintenance-nodes --remove consitentId1,consistentId2...consistentIdN

# Список нод в режиме обслуживания.
> ./control.sh --maintenance-nodes 
```

### Системные представления

В system-view NODES и команды отображающие состояния нод добавляется информация о том что нода в режиме обслуживания.

### Команды обслуживания 

В команды, которые должны обладать возможностью выполнения только на нодах в режиме обслуживания добавляются параметры:
  1. `--only-on-maintenance-nodes` - Запустить команду только на нодах в режиме обслуживания.
  2. `--nodes consistentId1,consistentId2...consistentIdN` - (если нету) Запустить команду на перечисленных нодах.

Список команд обслуживания:

  1. (?) `--cache idle-verify` - Verify counters and hash sums of primary and backup partitions.
  2. `--cache idle-verify --dump` - Calculate partition hash and print into standard output.
  3. `--cache validate_indexes` - Validates indexes.
  4. `--cache find_garbage` - Find and optionally delete garbage from shared cache groups which could be left after cache destroy.
  5. `--cache indexes_force_rebuild` - Triggers rebuild of all indexes for specified caches or cache groups.
  6. `--cache schedule_indexes_rebuild` - Schedules rebuild of the indexes for specified caches via the Maintenance Mode.
  7. `--warm-up --stop` - Stop warm-up.
  8. `--persistence clean corrupted` - Clean directories of caches with corrupted data files. 
  9. `--persistence clean all` - Clean directories of all caches.
  10. `--persistence clean caches` - Clean directories of only given caches.
  11. `--persistence backup corrupted` - Backup data files of corrupted caches only. 
  12. `--persistence backup all` - Backup data files of all caches.
  13. `--persistence backup caches` - Backup data files of only given caches.
  14. `--defragmentation schedule` - Schedule PDS defragmentation.
  15. `--cdc delete_lost_segment_links` - Delete lost segment CDC links.

## Ссылки

  1. [CRPV-53506 Реализовать ограниченный режим подключения к кластеру](https://sberworks.ru/jira/browse/CRPV-53506)
  2. [CRPV-75091 Частичный maintenance mode](https://sberworks.ru/jira/browse/CRPV-75091)
  3. [CRPV-44344 Поддержать maintenance mode](https://sberworks.ru/jira/browse/CRPV-44344)
  4. [CRPV-161490 Maintenance Mode: обслуживание DataGrid с ограничением клиентской нагрузки](https://sberworks.ru/jira/browse/CRPV-161490)
  5. [IGN-2453](https://portal.works.prod.sbt/swtr/units/current/unit/IGN-2453)

## Оценка сложности

Задача исключения нод из процесса обычной работы ОЧЕНЬ сложная и ОЧЕНЬ рискованная. 
Оценка в трудоднях - 200 ч.д

## Дополнение

Если часть нод находятся в режиме обслуживания, то команды должны ОБЯЗАТЕЛЬНО выполняться ТОЛЬКО на них.
Пример: после перевод некоторых нод в режим обслуживания команда `--cache destroy` должна быть выполнена ТОЛЬКО на них, если не указан параметр `--all-nodes`.

## Альтернативы

1. Обеспечить возможность выполнять команды control.sh на нодах которые находятся в текущем maintenance режим.
2. Риск того, что при проведении работ ноды будут тормозить прохождение сообщений по кольцу.