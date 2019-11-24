package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import de.hpi.ddm.MasterSystem;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintsMessage implements Serializable {
		private static final long serialVersionUID = 4443040942748609598L;
		private HashMap<String, Master.HintInfo> hints;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PasswordLengthMessage implements Serializable {
		private static final long serialVersionUID = 4453040942748609598L;
		private int passwordLength;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class SolveHintsMessage implements Serializable {
		private static final long serialVersionUID = 5553040942748609598L;
		private char[] charsToSearch;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CrackPasswordMessage implements Serializable {
		private static final long serialVersionUID = 5673040942748609598L;
		private Integer lineID;
		private String passwordHash;
		private char[] possibleChars;
	}


	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;

	private HashMap<String, Master.HintInfo> hints = new HashMap<String, Master.HintInfo>();
	private int passwordLength;
	private String password;

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(HintsMessage.class, this::handle)
				.match(PasswordLengthMessage.class, this::handle)
				.match(SolveHintsMessage.class, this::handle)
				.match(CrackPasswordMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}

	private void handle(HintsMessage message) {
		hints = message.getHints();
	}

	private void handle(PasswordLengthMessage message) {
		this.passwordLength = message.getPasswordLength();
	}

	private void handle(SolveHintsMessage message) {
		char[] charsToSearch = message.getCharsToSearch();

		List<String> charPermutations = new ArrayList<String>();
		this.heapPermutation(charsToSearch, charsToSearch.length, charsToSearch.length, charPermutations);
		ArrayList<String[]> solvedHints = new ArrayList<String[]>();

		for (String permutation : charPermutations) {
			String permutationHash = hash(permutation);
			if (hints.containsKey(permutationHash)) {
				String[] solvedHint = {permutationHash, permutation};
				// System.out.println(permutation + ": " + permutationHash);
				solvedHints.add(solvedHint);
			}
		}

		this.getSender().tell(new Master.HintsSolvedMessage(solvedHints), this.self());
	}

	private void handle(CrackPasswordMessage message) {
		Integer lineID = message.getLineID();
		String passwordHash = message.getPasswordHash();
		char[] possibleChars = message.getPossibleChars();		
		List<String> possiblePasswords = new ArrayList<String>();
		password = "";
		this.findPassword(possibleChars, passwordLength, "", passwordHash);
		this.getSender().tell(new Master.PasswordCrackedMessage(lineID, password), this.self());
		return;
	}
	
	private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, int n, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, n, l);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}

	private void findPassword(char[] possibleChars, int length, String password, String passwordHash)  {
		if (!this.password.equals("")) return;

		if (length == 0) {
			if (hash(password).equals(passwordHash)) this.password = password;
			return;
		}

		int numberOfPossibleChars = possibleChars.length;
		for (int i = 0; i < numberOfPossibleChars; i++) {
			findPassword(possibleChars, length - 1, password + possibleChars[i], passwordHash);
		}
	}
}