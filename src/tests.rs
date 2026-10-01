//! Facade behaviour against in-memory mock backends.
use crate::mock::{MockBackend, MockNetwork};
use crate::*;
use cmsh_api::async_trait;
use futures::{AsyncReadExt, AsyncWriteExt, SinkExt, StreamExt};
use std::collections::BTreeSet;
use std::sync::Arc;

fn spawner() -> Arc<dyn Spawn> {
    Arc::new(|future: BoxFuture<'static, ()>| {
        tokio::spawn(future);
    })
}

fn veilid_like() -> Capabilities {
    Capabilities {
        anonymous: true,
        datagrams: true,
        offline_delivery: true,
        dht: true,
        latency: LatencyClass::Interactive,
    }
}

fn tor_like() -> Capabilities {
    Capabilities {
        anonymous: true,
        datagrams: false,
        offline_delivery: false,
        dht: false,
        latency: LatencyClass::Interactive,
    }
}

fn direct_like() -> Capabilities {
    Capabilities {
        anonymous: false,
        datagrams: true,
        offline_delivery: false,
        dht: false,
        latency: LatencyClass::Realtime,
    }
}

fn scheme(name: &str) -> Scheme {
    Scheme::new(name).unwrap()
}

struct World {
    veilid: Arc<MockBackend>,
    tor: Arc<MockBackend>,
    direct: Arc<MockBackend>,
    mesh: Mesh,
}

/// Two meshes' worth of backends on one mock network: `peer` listens, the
/// returned world dials. `direct` (not anonymous) is deliberately first.
fn world(network: &MockNetwork, policy: Policy) -> World {
    let veilid = MockBackend::new(network, "veilid", veilid_like());
    let tor = MockBackend::new(network, "tor", tor_like());
    let direct = MockBackend::new(network, "direct", direct_like());
    let mesh = Mesh::builder(spawner())
        .backend(direct.clone())
        .backend(veilid.clone())
        .backend(tor.clone())
        .order([scheme("direct"), scheme("veilid"), scheme("tor")])
        .policy(policy)
        .build()
        .unwrap();
    World {
        veilid,
        tor,
        direct,
        mesh,
    }
}

async fn listening_peer(network: &MockNetwork) -> (World, Listening) {
    let peer = world(network, Policy::unrestricted());
    let listening = peer.mesh.listen().await.unwrap();
    (peer, listening)
}

async fn accept_connection(listening: &Listening) -> Connection {
    match listening.accept().await.unwrap() {
        Incoming::Connection(connection) => connection,
        Incoming::ListenerClosed(..) => panic!("listener closed"),
    }
}

#[tokio::test]
async fn anonymous_only_never_uses_a_non_anonymous_backend() {
    let network = MockNetwork::new();
    let (_peer, listening) = listening_peer(&network).await;
    let me = world(&network, Policy::anonymous_only());
    assert_eq!(me.mesh.active(), Ok(scheme("veilid")));
    let connection = me.mesh.dial(listening.addresses()).await.unwrap();
    assert_eq!(connection.backend(), &scheme("veilid"));
    assert!(connection.capabilities().anonymous);
    assert_eq!(me.direct.dial_count(), 0);
}

#[tokio::test]
async fn unrestricted_policy_follows_the_order() {
    let network = MockNetwork::new();
    let (_peer, listening) = listening_peer(&network).await;
    let me = world(&network, Policy::unrestricted());
    let connection = me.mesh.dial(listening.addresses()).await.unwrap();
    assert_eq!(connection.backend(), &scheme("direct"));
}

#[tokio::test]
async fn network_failure_switches_visibly() {
    let network = MockNetwork::new();
    let (_peer, listening) = listening_peer(&network).await;
    let me = world(&network, Policy::anonymous_only());
    me.veilid.set_down(true);
    let connection = me.mesh.dial(listening.addresses()).await.unwrap();
    assert_eq!(connection.backend(), &scheme("tor"));
    assert_eq!(me.mesh.active(), Ok(scheme("tor")));
    assert_eq!(
        me.mesh.take_switches(),
        vec![Switch {
            from: Some(scheme("veilid")),
            to: Some(scheme("tor")),
        }]
    );
    assert!(me.mesh.take_switches().is_empty());
    // Recovery is explicit and also recorded.
    me.veilid.set_down(false);
    me.mesh.report(&scheme("veilid"), Health::Healthy).unwrap();
    assert_eq!(me.mesh.active(), Ok(scheme("veilid")));
    assert_eq!(me.mesh.take_switches().len(), 1);
}

#[tokio::test]
async fn no_downgrade_when_every_anonymous_backend_fails() {
    let network = MockNetwork::new();
    let (_peer, listening) = listening_peer(&network).await;
    let me = world(&network, Policy::anonymous_only());
    me.veilid.set_down(true);
    me.tor.set_down(true);
    let error = me.mesh.dial(listening.addresses()).await.unwrap_err();
    assert_eq!(
        error,
        MeshError::Failed(vec![
            (scheme("veilid"), ErrorKind::Network),
            (scheme("tor"), ErrorKind::Network),
        ])
    );
    // Both are now reported failed: the next dial is refused outright.
    assert_eq!(
        me.mesh.dial(listening.addresses()).await.unwrap_err(),
        MeshError::Unavailable(Unavailable::AllAcceptableFailed)
    );
    assert_eq!(me.direct.dial_count(), 0);
    assert_eq!(me.mesh.active(), Err(Unavailable::AllAcceptableFailed));
}

#[tokio::test]
async fn operator_minimum_applies_even_with_an_unrestricted_policy() {
    let network = MockNetwork::new();
    let (_peer, listening) = listening_peer(&network).await;
    let direct = MockBackend::new(&network, "direct", direct_like());
    let tor = MockBackend::new(&network, "tor", tor_like());
    let mesh = Mesh::builder(spawner())
        .backend(direct.clone())
        .backend(tor.clone())
        .minimum([Property::Anonymous])
        .policy(Policy::unrestricted())
        .build()
        .unwrap();
    let connection = mesh.dial(listening.addresses()).await.unwrap();
    assert_eq!(connection.backend(), &scheme("tor"));
    assert_eq!(direct.dial_count(), 0);
}

#[tokio::test]
async fn unreachable_peer_moves_on_without_marking_the_network_failed() {
    let network = MockNetwork::new();
    let (_peer, listening) = listening_peer(&network).await;
    // The peer's Veilid address points nowhere; its Tor address is fine.
    let mut addresses: Vec<Address> = listening.addresses().to_vec();
    for address in &mut addresses {
        if address.scheme() == &scheme("veilid") {
            *address = Address::new(scheme("veilid"), b"stale-route".to_vec()).unwrap();
        }
    }
    let me = world(&network, Policy::anonymous_only());
    let connection = me.mesh.dial(&addresses).await.unwrap();
    assert_eq!(connection.backend(), &scheme("tor"));
    assert_eq!(me.mesh.active(), Ok(scheme("veilid")));
    assert!(me.mesh.take_switches().is_empty());
}

#[tokio::test]
async fn no_common_network() {
    let network = MockNetwork::new();
    let me = world(&network, Policy::anonymous_only());
    let only_direct = [Address::new(scheme("direct"), b"x".to_vec()).unwrap()];
    assert_eq!(
        me.mesh.dial(&only_direct).await.unwrap_err(),
        MeshError::NoCommonNetwork
    );
}

#[tokio::test]
async fn capabilities_are_surfaced() {
    let network = MockNetwork::new();
    let me = world(&network, Policy::anonymous_only());
    let status = me.mesh.status();
    let direct = status
        .iter()
        .find(|s| s.scheme == scheme("direct"))
        .unwrap();
    assert!(!direct.acceptable);
    assert_eq!(direct.missing, BTreeSet::from([Property::Anonymous]));
    let veilid = status
        .iter()
        .find(|s| s.scheme == scheme("veilid"))
        .unwrap();
    assert!(veilid.acceptable && veilid.missing.is_empty());
    assert_eq!(veilid.capabilities, veilid_like());

    let offered = me.mesh.offered().unwrap();
    assert!(offered.anonymous && offered.offline_delivery && offered.dht && offered.datagrams);
    assert_eq!(me.mesh.dht().unwrap().backend(), scheme("veilid"));
    assert_eq!(me.mesh.datagrams().unwrap().backend(), scheme("veilid"));

    // Without Veilid: no offline delivery, no DHT, no datagrams (Tor has
    // none, and the non-anonymous backend's datagrams are not acceptable).
    me.mesh.report(&scheme("veilid"), Health::Failed).unwrap();
    let offered = me.mesh.offered().unwrap();
    assert!(offered.anonymous && !offered.offline_delivery && !offered.dht && !offered.datagrams);
    assert_eq!(
        me.mesh.dht().err(),
        Some(MeshError::Unavailable(Unavailable::AllAcceptableFailed))
    );
    assert_eq!(
        me.mesh.datagrams().err(),
        Some(MeshError::Unavailable(Unavailable::AllAcceptableFailed))
    );
    me.mesh.report(&scheme("tor"), Health::Failed).unwrap();
    assert_eq!(me.mesh.offered(), None);
}

#[tokio::test]
async fn inconsistent_capability_declarations_are_rejected() {
    struct Liar(Arc<MockBackend>);
    #[async_trait]
    impl Backend for Liar {
        fn scheme(&self) -> Scheme {
            self.0.scheme()
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities {
                dht: true,
                ..tor_like()
            }
        }
        async fn listen(&self) -> Result<Box<dyn Listener>, Error> {
            self.0.listen().await
        }
        async fn dial(&self, address: &Address) -> Result<BoxStream, Error> {
            self.0.dial(address).await
        }
    }
    let network = MockNetwork::new();
    let liar = Liar(MockBackend::new(&network, "tor", tor_like()));
    let result = Mesh::builder(spawner()).backend(Arc::new(liar)).build();
    assert!(matches!(result, Err(MeshError::Config(_))));
}

#[tokio::test]
async fn configuration_errors() {
    let network = MockNetwork::new();
    let tor = MockBackend::new(&network, "tor", tor_like());
    let twice = Mesh::builder(spawner())
        .backend(tor.clone())
        .backend(tor.clone())
        .build();
    assert!(matches!(twice, Err(MeshError::Config(_))));
    let unknown = Mesh::builder(spawner())
        .backend(tor)
        .order([scheme("veilid")])
        .build();
    assert!(matches!(unknown, Err(MeshError::Config(_))));
}

#[tokio::test]
async fn multiplexed_substreams_carry_framed_messages() {
    let network = MockNetwork::new();
    let (_peer, listening) = listening_peer(&network).await;
    let me = world(&network, Policy::anonymous_only());
    let client = me.mesh.dial(listening.addresses()).await.unwrap();
    let server = accept_connection(&listening).await;
    assert_eq!(server.backend(), &scheme("veilid"));

    // Three concurrent substreams, echoed by the server.
    let echo = tokio::spawn(async move {
        for _ in 0..3 {
            let mut substream = server.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buffer = vec![0; 64];
                let read = substream.read(&mut buffer).await.unwrap();
                substream.write_all(&buffer[..read]).await.unwrap();
                substream.close().await.unwrap();
            });
        }
        server
    });
    let mut replies = Vec::new();
    for index in 0..3u8 {
        let mut substream = client.open().await.unwrap();
        substream.write_all(&[index; 5]).await.unwrap();
        substream.flush().await.unwrap();
        let mut reply = Vec::new();
        substream.read_to_end(&mut reply).await.unwrap();
        replies.push(reply);
    }
    assert_eq!(replies, vec![vec![0; 5], vec![1; 5], vec![2; 5]]);
    let server = echo.await.unwrap();

    // Length-delimited frames over a substream, both directions. yamux
    // announces a new substream with its first data, so send before accepting.
    let mut outbound = frame::framed(client.open().await.unwrap(), 1024).unwrap();
    outbound
        .send(bytes::Bytes::from_static(b"first"))
        .await
        .unwrap();
    outbound.send(bytes::Bytes::from_static(b"")).await.unwrap();
    let mut inbound = frame::framed(server.accept().await.unwrap(), 1024).unwrap();
    assert_eq!(&inbound.next().await.unwrap().unwrap()[..], b"first");
    assert_eq!(&inbound.next().await.unwrap().unwrap()[..], b"");
    inbound
        .send(bytes::Bytes::from_static(b"reply"))
        .await
        .unwrap();
    assert_eq!(&outbound.next().await.unwrap().unwrap()[..], b"reply");
    // Oversized frames are refused by the codec.
    assert!(
        outbound
            .send(bytes::Bytes::from(vec![0u8; 1025]))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn listening_uses_every_acceptable_backend() {
    let network = MockNetwork::new();
    let me = world(&network, Policy::anonymous_only());
    let listening = me.mesh.listen().await.unwrap();
    let schemes: Vec<&Scheme> = listening.addresses().iter().map(|a| a.scheme()).collect();
    assert_eq!(schemes, vec![&scheme("veilid"), &scheme("tor")]);
    assert_eq!(me.direct.dial_count(), 0);
}

#[tokio::test]
async fn datagrams_and_dht_through_the_facade() {
    let network = MockNetwork::new();
    let (peer, listening) = listening_peer(&network).await;
    let me = world(&network, Policy::anonymous_only());

    let port = me.mesh.datagrams().unwrap();
    port.send(listening.addresses(), b"ping").await.unwrap();
    let received = peer.veilid.datagrams().unwrap().receive().await.unwrap();
    assert_eq!(received, b"ping");

    let dht = me.mesh.dht().unwrap();
    let key = dht
        .dht()
        .create(
            &DhtSchema {
                owner_subkeys: 1,
                members: vec![],
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(dht.dht().get(&key, 0, false).await.unwrap(), None);
    dht.dht().set(&key, 0, b"record", None).await.unwrap();
    let value = dht.dht().get(&key, 0, true).await.unwrap().unwrap();
    assert_eq!(value.data, b"record");
    assert_eq!(value.seq, 0);
}

#[tokio::test]
async fn driver_cancellation_emits_one_session_event_without_success() {
    let io = Box::new(futures::io::Cursor::new(Vec::<u8>::new()));
    let (session, driver) = Session::new(io, Role::Dialer);
    let other = session.clone();
    use futures::FutureExt;
    assert!(session.next_event().now_or_never().is_none());
    drop(driver);
    assert_eq!(session.next_event().await, Some(SessionEvent::Closed));
    assert_eq!(other.next_event().await, None);
    assert!(session.open().await.is_err());
}

#[tokio::test]
async fn pending_opens_have_a_finite_budget() {
    use std::{future::Future, task::Context};
    let io = Box::new(futures::io::Cursor::new(Vec::<u8>::new()));
    let (session, driver) = Session::new(io, Role::Dialer);
    let mut pending = Vec::new();
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    for _ in 0..MAX_SUBSTREAMS {
        let mut call = Box::pin(session.open());
        assert!(call.as_mut().poll(&mut cx).is_pending());
        pending.push(call);
    }
    assert_eq!(session.open().await.unwrap_err().kind(), ErrorKind::Limit);
    drop(pending);
    drop(driver);
    assert!(session.open().await.is_err());
}
