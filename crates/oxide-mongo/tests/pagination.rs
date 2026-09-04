//! Paging and ordering in derived queries.
//!
//! Without them a derived query returns *every* match. That is fine for a
//! bounded collection and a denial of service against the caller for
//! anything else: this repository's own benchmark had one uncapped list
//! endpoint serialize ~6,700 documents per request against a 20k corpus, and
//! the example had to reach past the repository to the driver to fix it.

use mongodb::bson::doc;
use oxide_mongo::{Document, Page};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Document, PartialEq)]
#[collection("paged")]
struct Item {
    #[id]
    #[serde(rename = "_id")]
    id: String,
    tag: String,
    rank: i32,
}

// Two repositories over one document, because the *method name is the
// query*: `find_all_by_tag_paged` would make the macro look for a field
// called `tag_paged`. So the paged and unpaged shapes of the same query
// cannot share a trait, and comparing them needs two.

#[allow(dead_code, async_fn_in_trait)]
#[oxide_mongo::mongo_repository(document = Item)]
trait ItemRepository {
    async fn save(&self, item: &Item) -> oxide_mongo::Result<()>;
    async fn count(&self) -> oxide_mongo::Result<u64>;
    async fn find_all(&self) -> oxide_mongo::Result<Vec<Item>>;
    async fn find_all_by_tag(&self, tag: String) -> oxide_mongo::Result<Vec<Item>>;
}

#[allow(dead_code, async_fn_in_trait)]
#[oxide_mongo::mongo_repository(document = Item)]
trait PagedItemRepository {
    async fn find_all(&self, page: Page) -> oxide_mongo::Result<Vec<Item>>;
    async fn find_all_by_tag(&self, tag: String, page: Page) -> oxide_mongo::Result<Vec<Item>>;
}

struct Fixture {
    _test: oxide_mongo_test::MongoTest,
    plain: std::sync::Arc<ItemRepositoryImpl>,
    paged: std::sync::Arc<PagedItemRepositoryImpl>,
}

async fn boot() -> Fixture {
    let test = oxide_mongo_test::MongoTest::start().await;
    // The generated repository is not `Clone`; the container's handle is
    // what gets shared, which is what `Inject::into_arc` hands back.
    let plain = test.app().context().get::<ItemRepositoryImpl>().expect("repository").into_arc();
    let paged = test.app().context().get::<PagedItemRepositoryImpl>().expect("paged repository").into_arc();
    for i in 0..25 {
        plain.save(&Item { id: format!("i{i:02}"), tag: "t".into(), rank: i }).await.expect("seed");
    }
    Fixture { _test: test, plain, paged }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_page_limits_the_result() {
    let f = boot().await;
    assert_eq!(f.plain.find_all_by_tag("t".into()).await.unwrap().len(), 25, "the unpaged shape still returns all");
    assert_eq!(f.paged.find_all_by_tag("t".into(), Page::first(10)).await.unwrap().len(), 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn pages_do_not_overlap_when_sorted() {
    let f = boot().await;
    let sort = doc! { "rank": 1 };
    let first: Vec<i32> = f
        .paged
        .find_all_by_tag("t".into(), Page::of(0, 10).sorted_by(sort.clone()))
        .await
        .unwrap()
        .into_iter()
        .map(|i| i.rank)
        .collect();
    let second: Vec<i32> = f
        .paged
        .find_all_by_tag("t".into(), Page::of(1, 10).sorted_by(sort))
        .await
        .unwrap()
        .into_iter()
        .map(|i| i.rank)
        .collect();

    assert_eq!(first, (0..10).collect::<Vec<_>>());
    // Without a sort these two could overlap or skip documents entirely --
    // which is why `sorted_by` exists and why the docs push callers to it.
    assert_eq!(second, (10..20).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_last_page_is_short_rather_than_an_error() {
    let f = boot().await;
    let last = f.paged.find_all_by_tag("t".into(), Page::of(2, 10).sorted_by(doc! { "rank": 1 })).await.unwrap();
    assert_eq!(last.len(), 5);
    let past_the_end = f.paged.find_all_by_tag("t".into(), Page::of(9, 10)).await.unwrap();
    assert!(past_the_end.is_empty(), "reading past the end must be empty, not an error");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crud_find_all_takes_a_page_too() {
    let f = boot().await;
    assert_eq!(f.paged.find_all(Page::first(3)).await.unwrap().len(), 3);
    // `limit: 0` is Mongo's own "no limit", so a default Page returns
    // everything rather than nothing -- the opposite would silently empty
    // every caller that built a Page without setting one.
    assert_eq!(f.paged.find_all(Page::default()).await.unwrap().len(), 25);
    assert_eq!(f.plain.find_all().await.unwrap().len(), 25, "the unpaged CRUD shape is unchanged");
}

#[tokio::test(flavor = "multi_thread")]
async fn sorting_descending_reverses_the_page() {
    let f = boot().await;
    let top = f.paged.find_all_by_tag("t".into(), Page::first(3).sorted_by(doc! { "rank": -1 })).await.unwrap();
    assert_eq!(top.iter().map(|i| i.rank).collect::<Vec<_>>(), vec![24, 23, 22]);
}
