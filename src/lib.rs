pub mod error;

use futures::{stream, StreamExt};
use std::fmt;
use tokio::{macros::support::Future, net, sync::mpsc};
use tokio_util::codec;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub trait Matcher<T>
where
    T: 'static + Send + Sync + fmt::Debug,
{
    fn match_on(&self, event: T) -> Result<()>;
}

pub trait MatcherMut<T>
where
    T: 'static + Send + Sync + fmt::Debug,
{
    fn match_on_mut(&mut self, event: T) -> Result<()>;
}

pub trait Spawn {
    fn spawn_and_trace<F>(&self, f: F) -> tokio::task::JoinHandle<()>
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        tokio::spawn(async move {
            if let Err(err) = f.await {
                tracing::error!(err);
            }
        })
    }
}

#[derive(Debug)]
pub struct Broker<T>
where
    T: 'static + Send + Sync + fmt::Debug,
{
    sender: mpsc::UnboundedSender<T>,
    receiver: Option<mpsc::UnboundedReceiver<T>>,
}

impl<T> Broker<T>
where
    T: 'static + Send + Sync + fmt::Debug,
{
    pub fn new(
        sender: mpsc::UnboundedSender<T>,
        receiver: Option<mpsc::UnboundedReceiver<T>>,
    ) -> Self {
        Broker { sender, receiver }
    }

    pub fn send(&self, event: T) -> Result<()> {
        if self.receiver.is_some() {
            return Err(Box::new(error::BrokerError::new(
                error::BrokerErrorKind::Attached,
                "broker is still attached",
            )));
        };

        return match self.sender.send(event) {
            Ok(..) => Ok(()),
            Err(_) => Err(Box::new(error::BrokerError::new(
                error::BrokerErrorKind::SenderClosed,
                "broker sender is closed",
            ))),
        };
    }

    pub fn sender(&self) -> mpsc::UnboundedSender<T> {
        self.sender.clone()
    }

    pub fn detach<M>(&mut self, matcher: M)
    where
        M: 'static + Send + Sync + fmt::Debug + Matcher<T>,
    {
        if let Some(receiver) = self.receiver.take() {
            self.spawn_and_trace(broker(receiver, matcher));
        }
    }

    pub fn detach_mut<M>(&mut self, matcher: M)
    where
        M: 'static + Send + Sync + fmt::Debug + MatcherMut<T>,
    {
        if let Some(receiver) = self.receiver.take() {
            self.spawn_and_trace(broker_mut(receiver, matcher));
        }
    }
}

impl<T: Send + Sync + fmt::Debug> Spawn for Broker<T> {}

async fn broker<T, M>(mut receiver: mpsc::UnboundedReceiver<T>, matcher: M) -> Result<()>
where
    T: 'static + Send + Sync + fmt::Debug,
    M: 'static + Send + Sync + fmt::Debug + Matcher<T>,
{
    while let Some(event) = receiver.recv().await {
        matcher.match_on(event)?;
    }

    Ok(())
}

async fn broker_mut<T, M>(mut receiver: mpsc::UnboundedReceiver<T>, mut matcher: M) -> Result<()>
where
    T: 'static + Send + Sync + fmt::Debug,
    M: 'static + Send + Sync + fmt::Debug + MatcherMut<T>,
{
    while let Some(event) = receiver.recv().await {
        matcher.match_on_mut(event)?;
    }

    Ok(())
}

#[derive(Debug)]
pub struct StreamBroker<T>
where
    T: 'static + Send + Sync + fmt::Debug,
{
    sender: mpsc::UnboundedSender<T>,
    receiver: Option<mpsc::UnboundedReceiver<T>>,
    stream: Option<stream::SplitStream<codec::Framed<net::TcpStream, codec::LinesCodec>>>,
}

impl<T> StreamBroker<T>
where
    T: 'static + Send + Sync + fmt::Debug,
{
    pub fn send(&self, event: T) -> Result<()> {
        if self.receiver.is_some() {
            return Err(Box::new(error::BrokerError::new(
                error::BrokerErrorKind::Attached,
                "broker is still attached",
            )));
        };

        return match self.sender.send(event) {
            Ok(..) => Ok(()),
            Err(_) => Err(Box::new(error::BrokerError::new(
                error::BrokerErrorKind::SenderClosed,
                "broker sender is closed",
            ))),
        };
    }

    pub fn detach_mut<M, S>(&mut self, matcher: M, stream_matcher: S) -> Option<Result<()>>
    where
        M: 'static + Send + Sync + fmt::Debug + MatcherMut<T>,
        S: 'static + Send + Sync + fmt::Debug + Matcher<String>,
    {
        let stream = self.stream.take()?;
        let receiver = self.receiver.take()?;

        self.spawn_and_trace(stream_broker(stream, stream_matcher, receiver, matcher));

        Some(Ok(()))
    }
}

impl<T: Send + Sync + fmt::Debug> Spawn for StreamBroker<T> {}

async fn stream_broker<T, S, M>(
    mut stream: stream::SplitStream<codec::Framed<net::TcpStream, codec::LinesCodec>>,
    stream_matcher: S,
    mut receiver: mpsc::UnboundedReceiver<T>,
    mut matcher: M,
) -> Result<()>
where
    T: 'static + Send + Sync + fmt::Debug,
    S: 'static + Send + Sync + fmt::Debug + Matcher<String>,
    M: 'static + Send + Sync + fmt::Debug + MatcherMut<T>,
{
    loop {
        tokio::select! {
            Some(event) = receiver.recv() => {
                matcher.match_on_mut(event)?;
            }

            result = stream.next() => match result {
                Some(Ok(input)) => {
                    stream_matcher.match_on(input)?;
                }
                Some(Err(err)) => {
                    tracing::error!(
                        "an error occurred while processing messages; error = {:?}",
                        err
                    );
                },
                None => {
                    tracing::info!("Received EOF, shutting down.");
                    return Ok(());
                },
            }
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ResourceEvent<T>
where
    T: Send + Sync + fmt::Debug,
{
    Get(i64, tokio::sync::oneshot::Sender<ResourceEvent<T>>),
    GetSuccess(T),
    GetFail,
    Insert,
    InsertSuccess(T),
    InsertFail,
    Update,
    UpdateSuccess(T),
    UpdateFail,
}

#[derive(Debug)]
pub struct ResourceBroker<T>
where
    T: 'static + Send + Sync + fmt::Debug,
{
    sender: mpsc::UnboundedSender<ResourceEvent<T>>,
    receiver: Option<mpsc::UnboundedReceiver<ResourceEvent<T>>>,
}

impl<T> ResourceBroker<T>
where
    T: 'static + Send + Sync + fmt::Debug,
{
    pub fn send(&self, event: ResourceEvent<T>) -> Result<()> {
        if self.receiver.is_some() {
            return Err(Box::new(error::BrokerError::new(
                error::BrokerErrorKind::Attached,
                "broker is still attached",
            )));
        };

        return match self.sender.send(event) {
            Ok(..) => Ok(()),
            Err(_) => Err(Box::new(error::BrokerError::new(
                error::BrokerErrorKind::SenderClosed,
                "broker sender is closed",
            ))),
        };
    }

    pub fn detach_mut<M>(&mut self, matcher: M) -> Option<Result<()>>
    where
        M: 'static + Send + Sync + fmt::Debug + MatcherMut<ResourceEvent<T>>,
    {
        let receiver = self.receiver.take()?;

        self.spawn_and_trace(resource_broker_mut(receiver, matcher));

        Some(Ok(()))
    }
}

impl<T> Spawn for ResourceBroker<T> where T: Send + Sync + fmt::Debug {}

async fn resource_broker_mut<T, M>(
    mut receiver: mpsc::UnboundedReceiver<ResourceEvent<T>>,
    mut matcher: M,
) -> Result<()>
where
    T: 'static + Send + Sync + fmt::Debug,
    M: 'static + Send + Sync + fmt::Debug + MatcherMut<ResourceEvent<T>>,
{
    while let Some(event) = receiver.recv().await {
        matcher.match_on_mut(event)?;
    }

    Ok(())
}
