package de.hpi.ddm.actors;

import java.io.Serializable;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.serialization.*;
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
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
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
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private Serialization serialization = SerializationExtension.get(this.context().system());
	private Integer serializerId;
	private String manifest;

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
		
		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming.
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...

		byte[] bytes = serialization.serialize(message.getMessage()).get();
		serializerId = serialization.findSerializerFor(message.getMessage()).identifier();
		manifest = Serializers.manifestFor(serialization.findSerializerFor(message.getMessage()), message.getMessage());

		receiverProxy.tell(new BytesMessage<>(serializerId, this.sender(), message.getReceiver()), this.self());
		receiverProxy.tell(new BytesMessage<>(manifest, this.sender(), message.getReceiver()), this.self());
		receiverProxy.tell(new BytesMessage<>(bytes, this.sender(), message.getReceiver()), this.self());

		/* for (int i = 0; i < bytes.length; i++) {
			receiverProxy.tell(new BytesMessage<>(message.getMessage(), this.sender(), message.getReceiver()), this.self());
		} */

	}

	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		if (serializerId == null) serializerId = (Integer) message.getBytes();
		else if (manifest == null) manifest = (String) message.getBytes();
		else {
			message.getReceiver().tell(serialization.deserialize( (byte[]) message.getBytes(), serializerId, manifest).get(), message.getSender());
		}
	}
}
