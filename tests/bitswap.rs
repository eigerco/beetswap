use blockstore::{Blockstore, InMemoryBlockstore};
use futures::{future::FutureExt, poll};
use tokio::time::{sleep, Duration};

use crate::utils::{cid, spawn_node};

mod utils;

#[tokio::test]
async fn test_client_request() {
    let data = "foo";
    let cid = cid(data.as_bytes());
    let store = InMemoryBlockstore::new();
    store.put_keyed(&cid, data.as_ref()).await.unwrap();

    let server = spawn_node(Some(store)).await;
    let mut client = spawn_node(None).await;

    let _ = client.connect(&server);
    let received = client.request_cid(cid).await.expect("could not get CID");

    assert_eq!(&received[..], data.as_bytes());
}

#[tokio::test]
async fn test_server_request() {
    let data = "foo";
    let cid = cid(data.as_bytes());
    let store = InMemoryBlockstore::new();
    store.put_keyed(&cid, data.as_ref()).await.unwrap();

    let mut client = spawn_node(Some(store)).await;
    let mut server = spawn_node(None).await;

    let _ = client.connect(&server);
    let received = server.request_cid(cid).await.expect("could not get CID");

    assert_eq!(&received[..], data.as_bytes());
}

#[tokio::test]
async fn test_chain_of_nodes() {
    let data = "foo";
    let cid = cid(data.as_bytes());
    let store = InMemoryBlockstore::new();
    store.put_keyed(&cid, data.as_ref()).await.unwrap();

    let mut node_with_data = spawn_node(Some(store)).await;
    let mut node0 = spawn_node(None).await;
    let mut node1 = spawn_node(None).await;
    let mut node2 = spawn_node(None).await;

    let _ = node_with_data.connect(&node0);
    let _ = node0.connect(&node1);
    let _ = node1.connect(&node2);

    let mut node2_request = node2.request_cid(cid);
    sleep(Duration::from_millis(100)).await;
    assert!(poll!(&mut node2_request).is_pending());

    let mut node1_request = node1.request_cid(cid);
    sleep(Duration::from_millis(100)).await;
    assert!(poll!(&mut node1_request).is_pending());
    assert!(poll!(&mut node2_request).is_pending());

    let node0_request = node0.request_cid(cid);
    sleep(Duration::from_millis(200)).await;
    let data0 = node0_request
        .now_or_never()
        .expect("data should be ready")
        .unwrap();
    let data1 = node1_request
        .now_or_never()
        .expect("data should be ready")
        .unwrap();
    let data2 = node2_request
        .now_or_never()
        .expect("data should be ready")
        .unwrap();

    assert_eq!(data0, data.as_bytes());
    assert_eq!(data1, data.as_bytes());
    assert_eq!(data2, data.as_bytes());
}

#[tokio::test]
async fn test_node_with_data_coming_online() {
    let data = "foo";
    let cid = cid(data.as_bytes());
    let store = InMemoryBlockstore::new();
    store.put_keyed(&cid, data.as_ref()).await.unwrap();

    let node_with_data = spawn_node(Some(store)).await;

    let mut node0 = spawn_node(None).await;
    let mut node1 = spawn_node(None).await;
    let _ = node0.connect(&node1);

    let mut node0_request = node0.request_cid(cid);
    let mut node1_request = node1.request_cid(cid);
    sleep(Duration::from_millis(100)).await;
    assert!(poll!(&mut node0_request).is_pending());
    assert!(poll!(&mut node1_request).is_pending());

    node1
        .connect(&node_with_data)
        .await
        .expect("could not connect");
    sleep(Duration::from_millis(200)).await;

    let data0 = node0_request
        .now_or_never()
        .expect("data should be ready")
        .unwrap();
    let data1 = node1_request
        .now_or_never()
        .expect("data should be ready")
        .unwrap();
    assert_eq!(data0, data.as_bytes());
    assert_eq!(data1, data.as_bytes());
}

#[tokio::test]
async fn test_node_with_invalid_data() {
    let data = "foo";
    let cid = cid(data.as_bytes());
    let store = InMemoryBlockstore::new();
    store.put_keyed(&cid, data.as_ref()).await.unwrap();

    let invalid_data = "bar";
    let invalid_store = InMemoryBlockstore::new();
    invalid_store
        .put_keyed(&cid, invalid_data.as_ref())
        .await
        .unwrap();

    let mut client = spawn_node(None).await;
    let node = spawn_node(Some(store)).await;
    let malicious_node = spawn_node(Some(invalid_store)).await;

    client
        .connect(&malicious_node)
        .await
        .expect("could not connect");
    let mut request = client.request_cid(cid);
    sleep(Duration::from_millis(100)).await;
    assert!(poll!(&mut request).is_pending());

    let _ = client.connect(&node);
    let received = request.await.expect("could not get CID");
    assert_eq!(received, data.as_bytes());
}
