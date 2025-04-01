MVideo Price Monitor

Проект для мониторинга цен на смартфоны в магазине MVideo с возможностью анализа динамики цен.

## 📌 Функционал

- **Скрапинг данных** о смартфонах с [MVideo](https://www.mvideo.ru/smartfony-i-svyaz-10/smartfony-205)
- **Визуализация данных** (графики распределения цен)
- **Интеграция с Prefect** для автоматизации сбора данных

## ⚙️ Установка

1. Клонируйте репозиторий:
```bash
https://github.com/fa2fin/fa2finMvideo.git
cd mvideo-scraper
```
2. Установите зависимости:

```bash
pip install -r requirements.txt
```

2. Убедитесь, что у вас установлен:

- **Chrome Browser**
- **ChromeDriver по пути C:\Program Files\Google\Chrome\Application\chromedriver.exe**

🚀 Запуск

- Ручной запуск скрапера
```bash
python -m scraper.main
Результат сохраняется в data/latest_scrape.json
```

- Автоматизация через Prefect

Запустите Prefect сервер:
```bash
prefect server start
```
В другом терминале запустите worker:

```bash
prefect worker start -t process
```
Если воркер будет ругаться на API, то надо в терминале перед запуском написать
```bash
$env:PREFECT_API_URL="http://127.0.0.1:4200/api"
echo $env:PREFECT_API_URL
```

Запустите flow:

```bash
python -m flows.monitoring
```
🚀 Запуск аналитики по дням выдаст тупо кол-во айтемов, макимальную и минимальную цену, и график по ценам, на большее у меня пока не хватило мозгов

```bash
python -m scraper.analyzedata
```

