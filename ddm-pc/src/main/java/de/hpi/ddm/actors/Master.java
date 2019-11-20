package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintsSolvedMessage implements Serializable {
		private static final long serialVersionUID = 7763040942748609598L;
		private ArrayList<String[]> solvedHints;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PasswordCrackedMessage implements Serializable {
		private static final long serialVersionUID = 7763040942748609598L;
		private String lineID;
		private String crackedPassword;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;

	private long startTime;

	private String possiblePasswordChars;
	private int numberOfPossiblePasswordChars;
	private int passwordLength;
	private int numberOfHints;
	private int numberOfHintsToCrack;

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintInfo {
		private char hintChar;
		private String[] IDList;
	}

	private HashMap<String, HintInfo> hints = new HashMap<String, HintInfo>();
	// for passwords: String[] = {passwordHash, crackedPassword, possibleChars}
	private HashMap<String, String[]> passwords = new HashMap<String, String[]>();

	private HashMap<String, Boolean> searchedChars = new HashMap<String, Boolean>();
	private String[] charCombinationsToSearch;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(HintsSolvedMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void handle(BatchMessage message) {
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////

		List<String[]> lines = message.getLines();
		String[] firstLine = lines.get(0);

		String possiblePasswordChars = firstLine[2]; // get possible password chars
		int numberOfPossiblePasswordChars = possiblePasswordChars.length();
		// int numberOfPossibleHints = factorial(numberOfPossiblePasswordChars - 1);
		int passwordLength = Integer.parseInt(firstLine[3]); // get password length
		// int numberOfPossiblePasswords = numberOfPossiblePasswordChars ** passwordLength;
		int numberOfHints = firstLine.length - 5; // get number of hints

		// calculate minNumberOfHints before crack password (maxNumberOfHints < verfügbare Hints)
		int numberOfHintsToCrack = numberOfHints;

		System.out.println(possiblePasswordChars);
		System.out.println(passwordLength);
		System.out.println(numberOfHints);

		charCombinationsToSearch = new String[numberOfPossiblePasswordChars];
		for (int i = 0; i < numberOfPossiblePasswordChars; i++) {
			String charsToSearch = possiblePasswordChars.slice(0, i) + possiblePasswordChars.slice(i+1, numberOfPossiblePasswordChars);
			charCombinationsToSearch[i] = charsToSearch;
		}

		for (String[] line : lines) {
			// create 1. hints with all hints, and ArrayList with crackedHint (empty at start) and List of line-IDs !!!
			for (int hintNr = 4; hintNr < (numberOfHints + 4); hintNr++) {
				if(!hints.containsKey(line[hintNr])) {
					HintInfo crackedHintPlusIDs = new HintInfo(null, line[0]);
					hints.put(line[hintNr], crackedHintPlusIDs);
				} else {
					HintInfo crackedHintPlusIDs = hints.get(line[hintNr]);
					crackedHintPlusIDs.getIDList.add(line[0]);
					hints.put(line[hintNr], crackedHintPlusIDs);
				}
			}

			// create 2. hashmap with line-IDs, password-hash, possible chars
			String[] passwordInfo = new String[3];
			passwordInfo[0] = line[4];
			passwordInfo[1] = null;
			passwordInfo[2] = possiblePasswordChars;
			passwords.put(line[0], passwordInfo);
		}



		if (message.getLines().isEmpty()) {
			// 1. for every possible password char:
			//		send Worker message with (char, hints)
			this.distributeWork();

			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
		}
		
		for (String[] line : message.getLines())
			System.out.println(Arrays.toString(line));
		
		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	protected void distributeWork() {
		for (ActorRef worker : workers) {
			worker.tell(new Worker.HintsMessage(hints), this.self());
			this.sendWork(worker);
		}
	}

	protected void sendWork(ActorRef worker) {
		// check whether work left
		worker.tell(new Worker.SolveHintsMessage(charCombinationsToSearch.pop()), this.self());
	}
	
	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
//		this.log().info("Registered {}", this.sender());
	}

	protected void handle(HintsSolvedMessage message) {
		ActorRef worker = this.getSender();
		ArrayList<String[]> solvedHints = message.getSolvedHints();
		
		for(String[] solvedHint : solvedHints) {
			HintInfo hint = hints.get(solvedHint[0]);
			char solvedHintChar = this.getSolvedHintChar(solvedHint[1]);
			hint.setHintChar(solvedHintChar);
			hints.put(solvedHint[0], hint);
			String[] affectedPasswordLines = hint.getIDList();
			for(String line : affectedPasswordLines) {
				String[] passwordInfo = passwords.get(line);
				passwordInfo[1].remove(solvedHintChar);
				if(passwordInfo[1].length() <= minPossibleChars && passwordInfo[1] == null) {
					worker.tell(new Worker.CrackPasswordMessage(line, passwordInfo[0], passwordInfo[2]), this.self());
				} else {
					this.sendWork(worker);
				}
			}
		}
	}

	protected char getSolvedHintChar(String solvedHint) {
		for(char possibleChar : possiblePasswordChars) {
			if (!solvedHint.contains(Character.toString(possibleChar))) {
				return possibleChar;
			}
		}
	}

	protected void handle(PasswordCrackedMessage message) {
		String lineID = message.getLineID();
		String[] passwordInfo = passwords.get(lineID);
		passwordInfo[1] = message.getCrackedPassword();
		passwords.put(lineID, passwordInfo);
		// check whether work left
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());

		// TODO: put characters into search again
	}
}
