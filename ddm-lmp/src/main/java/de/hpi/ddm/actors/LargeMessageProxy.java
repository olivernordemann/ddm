package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.UUID;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.serialization.*;
import jdk.nashorn.internal.ir.Block;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessageInitializer implements Serializable {
		private static final long serialVersionUID = 3444507743872319842L;
		private String largeMessageID;
		private Integer serializerID;
		private String manifest;
		private Integer sequenceLength;
		private ActorRef sender;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessageReady implements Serializable {
		private static final long serialVersionUID = 3337807743872319842L;
		private String largeMessageID;
		private Integer sequenceID;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private String largeMessageID;
		private Integer sequenceID;
		private T bytes;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	final private int BlockSize = 32768;
	Serialization serialization = SerializationExtension.get(this.context().system());

	private ConcurrentHashMap<String,Integer> serializerID = new ConcurrentHashMap<String,Integer>();
	private ConcurrentHashMap<String,String> manifest = new ConcurrentHashMap<String,String>();
	private ConcurrentHashMap<String,Integer> sequenceLength = new ConcurrentHashMap<String,Integer>();
	private ConcurrentHashMap<String,ActorRef> sender = new ConcurrentHashMap<String,ActorRef>();
	private ConcurrentHashMap<String,ActorRef> receiverMap = new ConcurrentHashMap<String,ActorRef>();
	private ConcurrentHashMap<String,byte[][]> arrayOfByteArray = new ConcurrentHashMap<String,byte[][]>();
	private ConcurrentHashMap<String,byte[]> receivedBytes = new ConcurrentHashMap<String,byte[]>();
	private ConcurrentHashMap<String,Integer> receivedChunks = new ConcurrentHashMap<String,Integer>();
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(LargeMessageInitializer.class, this::handle)
				.match(BytesMessageReady.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private byte[][] createArrayOfByteArray(byte[] bytes) {
		byte[][] arrayOfChunks = new byte[(int)Math.ceil(bytes.length / (double)BlockSize)][BlockSize];
		int start = 0;
		for(int i = 0; i < arrayOfChunks.length; i++) {
			int end = bytes.length > (start + BlockSize) ? (start + BlockSize) : bytes.length;
			arrayOfChunks[i] = Arrays.copyOfRange(bytes, start, end);
			start += BlockSize;
		}
		return arrayOfChunks;
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
		String largeMessageID = UUID.randomUUID().toString();
		byte[] bytes = serialization.serialize(message.getMessage()).get();
		arrayOfByteArray.put(largeMessageID, createArrayOfByteArray(bytes));

		receiverProxy.tell(new LargeMessageInitializer(
			largeMessageID,
			serialization.findSerializerFor(message.getMessage()).identifier(),
			Serializers.manifestFor(serialization.findSerializerFor(message.getMessage()), message.getMessage()),
			bytes.length,
			this.sender(),
			receiver
		), this.self());

	}

	private void handle(LargeMessageInitializer message) {
		String largeMessageID = message.getLargeMessageID();

		serializerID.put(largeMessageID, message.getSerializerID());
		manifest.put(largeMessageID, message.getManifest());
		sequenceLength.put(largeMessageID, message.getSequenceLength());
		sender.put(largeMessageID, message.getSender());
		receiverMap.put(largeMessageID, message.getReceiver());
		receivedBytes.put(largeMessageID, new byte[sequenceLength.get(largeMessageID)]);
		receivedChunks.put(largeMessageID, 0);

		this.sender().tell(new BytesMessageReady(largeMessageID, 0), this.self());
	}

	private void handle(BytesMessageReady message) {
		String largeMessageID = message.getLargeMessageID();
		Integer sequenceID = message.getSequenceID();

		this.sender().tell(new BytesMessage<>(
			largeMessageID,
			sequenceID,
			arrayOfByteArray.get(largeMessageID)[sequenceID]
		), this.self());
	}

	private void handle(BytesMessage<?> message) {
		String largeMessageID = message.getLargeMessageID();
		byte[] messageReceivedBytes = (byte[]) message.getBytes();
		Integer sequenceID = message.getSequenceID();

		for (int i = 0; i < messageReceivedBytes.length; i++) {
			byte recByte = messageReceivedBytes[i];
			receivedBytes.get(largeMessageID)[sequenceID * BlockSize + i] = recByte;
		}

		receivedChunks.put(largeMessageID, receivedChunks.get(largeMessageID) + 1);

		if(receivedChunks.get(largeMessageID) * BlockSize >= sequenceLength.get(largeMessageID)) {
			receiverMap.get(largeMessageID).tell(
				serialization.deserialize(
					receivedBytes.get(largeMessageID),
					serializerID.get(largeMessageID),
					manifest.get(largeMessageID)
				).get(),
			sender.get(largeMessageID));
		} else {
			Integer nextSequenceID = sequenceID + 1;
			this.sender().tell(new BytesMessageReady(largeMessageID, nextSequenceID), this.self());
		}
	}
}
