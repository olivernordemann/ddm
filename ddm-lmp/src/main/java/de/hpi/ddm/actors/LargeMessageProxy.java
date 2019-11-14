package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.Arrays;

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
	public static class LargeMessageInitializer {
		private Serialization serialization;
		private Integer serializerId;
		private String manifest;
		private Integer sequenceLength;
		private ActorRef sender;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private Integer sequenceNumber;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private Serialization serialization;
	private Integer serializerId;
	private String manifest;
	private Integer sequenceLength;
	private ActorRef sender;
	private ActorRef receiver;
	final private int BlockSize = 1024;
	
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
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	public static byte[][] divideArray(byte[] source, int chunksize) {
		byte[][] ret = new byte[(int)Math.ceil(source.length / (double)chunksize)][chunksize];
		int start = 0;
		for(int i = 0; i < ret.length; i++) {
			ret[i] = Arrays.copyOfRange(source,start, start + chunksize);
			start += chunksize ;
		}
		return ret;
	}

	/*private int getNumberOfChunks(byte[] bytes) {
		return bytes.length/BlockSize;
	}*/

	private byte[][] createArrayOfByteArray(byte[] bytes) {
		//int numberOfChunks=getNumberOfChunks(bytes);
		byte[][] arrayOfChunks = new byte[(int)Math.ceil(bytes.length / (double)BlockSize)][BlockSize];
		int start = 0;
		for(int i = 0; i < arrayOfChunks.length; i++) {
			arrayOfChunks[i] = Arrays.copyOfRange(bytes, start, start + BlockSize);
			start += BlockSize;
		}
		return arrayOfChunks;
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
		
		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming.
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...

		serialization = SerializationExtension.get(this.context().system());
		byte[] bytes = serialization.serialize(message.getMessage()).get();
		serializerId = serialization.findSerializerFor(message.getMessage()).identifier();
		manifest = Serializers.manifestFor(serialization.findSerializerFor(message.getMessage()), message.getMessage());

		byte[][] arrayOfByteArray = createArrayOfByteArray(bytes);

		receiverProxy.tell(new LargeMessageInitializer(
			serialization,
			serializerId,
			manifest,
			bytes.length,
			this.sender(),
			message.getReceiver()
		), this.self());

		for(byte[] innerBytes : arrayOfByteArray) {
			receiverProxy.tell(new BytesMessage<>(innerBytes, 0), this.self());
		}
		/* for (int i = 0; i < bytes.length; i++) {
			receiverProxy.tell(new BytesMessage<>(message.getMessage(), this.sender(), message.getReceiver()), this.self());
		} */

	}

	private void handle(LargeMessageInitializer message) {
		serialization = message.getSerialization();
		serializerId = message.getSerializerId();
		manifest = message.getManifest();
		sequenceLength = message.getSequenceLength();
		sender = message.getSender();
		receiver = message.getReceiver();
	}

	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		receiver.tell(
			serialization.deserialize(
				(byte[]) message.getBytes(),
				serializerId,
				manifest
			).get(),
		sender);
	}
}
