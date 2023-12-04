---
title: "BigQuery で GA のテーブルをクラスタ化テーブルにしてコスト削減した話" # 記事のタイトル　（ここで書くタイトルは記事に表示されない）
emoji: "📊" # アイキャッチとして使われる絵文字（1文字だけ）。ご自身の好きなもの。
type: "tech" # tech: 技術記事 / idea: アイデア記事。どちらでもOK
topics: ["BigQuery", "GA", "dbt", "Luup"] # タグ。["markdown", "rust", "aws"]のように指定する。"Luup"を最低限入れる。
publication_name: "luup_developers"
published: true # 公開設定（trueにしてmainブランチにマージすると一般公開開始）
published_at: 2023-12-04 09:00 # 未来の日時（日本時間）を指定すればその日時に予約公開されます
---

## 概要

これは [Luup Advent Calendar 2023](https://adventar.org/calendars/8953) の 4 日目の記事です。

BigQuery のクエリコスト削減のために、Google Analytics のテーブルをクラスタ化テーブルにしたという内容です。

## 背景

こんにちは、データエンジニアリングチームの辻尾です。

データエンジニアリングチームは社員 2 名、業務委託 1 名の体制で回しており、私は業務委託として携わっています。

Luup では Data Warehouse に BigQuery を採用しており、オンデマンド料金体系で利用しています。

Luup ではデータ分析は事業拡大の重要な要素の一つであり、どんどんクエリを発行してトライアンドエラーしていきたい一方、おおよその BigQuery 利用料の目安は計画の中で定めているのでその中に収まるようにするのはデータエンジニアリングチームの責務の一つです。

その一環として BigQuery で発行されたクエリとその課金額を日々モニタリングしているのですが、ある日その課金額が急に上昇していることに気が付きました。

BigQuery では `INFORMATION_SCHEMA.JOBS` でクエリごとの課金額を調べられますが、どうも Google Analytics のデータを格納したテーブルを使うクエリの課金額が高いようでした。

Luup では Google Analytics のデータを BigQuery に連携して SQL で分析できるようにしています。サービスの利用者が増え、Google Analytics から BigQuery に転送されるデータ量が大きくなるにつれ、関連するクエリのスキャン量も増えて課金額が大きくなっていきます。モニタリングで「急に」課金額が増えていたのは新たな切り口で Google Analytics のデータを分析するために普段より多めにクエリを発行していたためでした。

新たな切り口の分析を試そうとする度に専用のデータマートテーブルを用意するのは今のデータエンジニアリングチームのリソース的にトライアンドエラーのスピードが落ちてしまいます。

しかしこうした挑戦的なデータ分析を抑制せずさらに進めるために、Google Analytics のクエリスキャン量を低くするためにはどうすればよいか検討してみました。

## 対応方針

ダッシュボード化して定期的に見る固定的なクエリならデータマートテーブルを用意すればよいですが、今回対象にしているのはまだ試行錯誤の段階のアドホックなクエリです。またうまくいけばデータマートテーブルを作る際のクエリコストも削減できるかもしれません。

改めて Google Analytics のテーブルを使うクエリを観察してみると、次のような特徴があることが分かりました。

1. 必要なカラムのみを SELECT している (ワイルドカード `*` を使っていたりしない)
2. 必要な期間のデータだけ取得するように絞っている (全期間のデータを取得というようなことはしていない)
3. 必要なイベント (`event_name`) のみを抽出している

BigQuery のクエリコスト削減ではまず 1. と 2. をチェックするのが定石ですが、今回は 2 点とも問題ありませんでした。

一方で 3. は注目すべき点です。Google Analytics ではクライアント側で発生した様々なイベントの種別 (ページビュー、ボタンクリック等) が `event_name` カラムに格納されますが、分析者はまず興味のあるイベントのみに絞ってデータを取得するクエリを書いていることが分かりました。

BigQuery は列指向でデータを格納しているため、パーティション列以外のカラム (`event_name`) でフィルタリングしても通常はクエリのスキャン量は削減されません。つまり分析者が欲しいのは `event_name` が特定の値を持つレコードのみであるのに、関係ないレコードまでスキャンされて課金されてしまいます。

そこで `event_name` でフィルタリングするクエリのコストを削減するために次の 2 つの方針を考えました。

- A : `event_name` ごとにテーブルを分割する
- B : `event_name` でテーブルをクラスタリングする

A は例えば `event_name = "page_view"` であるレコードをあらかじめ抽出して `ga_events_page_view` という名前のテーブル (つまり `event_name` をテーブル名の Suffix にする) として作成しておくという案です。これを `event_name` の数だけ繰り返してテーブルを作成しておくことで、例えばクエリは次のように書けます。

```sql
SELECT
  COUNT(*)
FROM
  `DATASET.ga_events_*`
WHERE
  _TABLE_SUFFIX IN ("page_view", "click")
  AND
  date = CURRENT_DATE()  -- date はパーティション列
```

B は `event_name` カラムでクラスタリングしたテーブルを別途作り、元のテーブルからデータをコピーするという案です。この場合はクエリは元の Google Analytics のテーブルを使う場合と変わらず次のように書けます。元のテーブルと同じくシャーディングテーブルにしているのは、利用者がクエリの変更をテーブル名の修正のみで済むようにするためです。

```sql
SELECT
  COUNT(*)
FROM
  `DATASET.ga_events_*`
WHERE
  _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", CURRENT_DATE())
  AND
  event_name IN ("page_view", "click")
```

テーブルの利用者目線では A と B は双方、メリットとデメリットがあります。

|       | メリット                               | デメリット                                                  |
|-------|----------------------------------------|-------------------------------------------------------------|
| **A** | スキャン量がクエリ実行前に分かる       | クエリの書き方が元のテーブルから変わる                      |
| **B** | クエリの変更はテーブル名を変更するだけ | スキャン量がクエリ実行前に分からない (クラスタリングの仕様) |

また運用者目線では A は「テーブルの作成にコストがかかる」「実装が B よりは複雑になる」というデメリットがありました。

これらを踏まえて議論した結果、B の方がメリットが大きいと判断し、実装することになりました。

## 実装

実装は dbt で行いました。Luup では最近データパイプラインに dbt を取り入れました。

```sql
{%- set table_suffix = run_started_at.strftime("%Y%m%d") -%}

{{ config(
    materialized="table",
    partition_by={
        "field": "timestamp",
        "data_type": "timestamp",
        "granularity": "hour",
    },
    cluster_by="event_name",
    alias="ga_events_" + table_suffix,
    tags="ga",
) }}

SELECT
  TIMESTAMP_MICROS(event_timestamp) AS timestamp,
  *
FROM
  {{ source("analytics_xxxxxxxxx", "events") }}
WHERE
  _TABLE_SUFFIX = "{{ table_suffix }}"
```

注意点は次の通りです。

- BigQuery ではワイルドカードを使用したクエリでクラスタリングが効果を発揮するにはパーティションが設定されている必要があるため、日付ごとのシャーディングテーブルですがパーティションを設定しています
- dbt でこのテーブル (モデル) を参照する場合は `ref` でなく `source` を使う必要があります。テーブル名を `ga_events_*` と展開させるためです。 `source` で参照するため、dbt はモデルの依存関係からテーブルの作成順序を把握できないので、作成順序は dbt とは別の仕組み (AirFlow 等) で定義する必要があります。

AirFlow で依存関係を定義する例は次の通りです。Luup では dbt を AirFlow で実行するために Cosmos を利用しています。

```python
from airflow.decorators import dag
from cosmos import DbtTaskGroup, RenderConfig


@dag(...)
def run_dbt():
    # tags="ga" のモデルのみを実行
    ga = DbtTaskGroup(
        ...
        render_config=RenderConfig(select=["config.tags:ga"])
    )

    # tags="daily" のモデルのみを実行 (あるモデルが source() で GA のクラスタ化テーブルを参照している想定)
    daily = DbtTaskGroup(
        ...
        render_config=RenderConfig(select=["config.tags:daily"])
    )

    # "daily" は "ga" の完了後に実行
    ga >> daily
```

こうしてクラスタリングされた Google Analytics のテーブルがデータ基盤に取り入れられました。

## 結果

クラスタ化テーブルの導入後の GA データのスキャン量の推移は次のようになりました。

クラスタ化テーブルを提供開始してから徐々にスキャン量は減少し始め、直近の日付では提供開始前と比較しておよそ 1/5 ほどのスキャン量に抑えられています。

![GA データのスキャン量の推移](/images/data-tsujio-20231126/cost.png)

また狙っていたクエリの書き換えの容易さは、クラスタ化テーブルの紹介を Slack で行った後に次のような反応を頂きました。

↓ 「使用テーブルを差し替える」だけでスキャン量が「1/5ぐらいになって」た様子
![Slack の反応 1](/images/data-tsujio-20231126/slack2.png)

↓ 便利なテーブルを提供して喜んでもらえた様子
![Slack の反応 2](/images/data-tsujio-20231126/slack1.png)

## まとめ

BigQuery のクエリコストを削減するために、実際に打たれているクエリの観察からクラスタリングという対応策を考え、実装しました。

また作成するテーブルのスキーマや仕様をテーブル利用者目線で検討し、良い反応をもらえました。

Luup ではチーム間の連携を重視し、より大きなパワーを発揮できる環境にしていこうとする文化があると感じます。

今回はそれに少し貢献できたかなと思う事例でした。

## 参考文献

- BigQuery
  - [料金  |  BigQuery: クラウド データ ウェアハウス  |  Google Cloud](https://cloud.google.com/bigquery/pricing)
  - [分割テーブルの概要  |  BigQuery  |  Google Cloud](https://cloud.google.com/bigquery/docs/partitioned-tables)
  - [クエリ計算の最適化  |  BigQuery  |  Google Cloud](https://cloud.google.com/bigquery/docs/best-practices-performance-compute)
  - [BigQuery のストレージを最適化する  |  Google Cloud](https://cloud.google.com/bigquery/docs/best-practices-storage)
  - [JOBS ビュー  |  BigQuery  |  Google Cloud](https://cloud.google.com/bigquery/docs/information-schema-jobs)
  - [BigQuery ストレージの概要  |  Google Cloud](https://cloud.google.com/bigquery/docs/storage_overview)
  - [ワイルドカード テーブルを使用した複数テーブルに対するクエリ  |  BigQuery  |  Google Cloud](https://cloud.google.com/bigquery/docs/querying-wildcard-tables)
- Google Analytics
  - [\[GA4\] BigQuery Export スキーマ - アナリティクス ヘルプ](https://support.google.com/analytics/answer/7029846)
- dbt
  - [Custom aliases | dbt Developer Hub](https://docs.getdbt.com/docs/build/custom-aliases)
  - [BigQuery configurations | dbt Developer Hub](https://docs.getdbt.com/reference/resource-configs/bigquery-configs)
  - [About run_started_at variable | dbt Developer Hub](https://docs.getdbt.com/reference/dbt-jinja-functions/run_started_at)
- Cosmos
  - [Cosmos by Astronomer](https://www.astronomer.io/cosmos/)
