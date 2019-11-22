package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableSet;

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

	private ArrayList<Character> possiblePasswordChars = new ArrayList<Character>();
	private int numberOfPossiblePasswordChars;
	private int passwordLength;
	private int numberOfHints;
	private int minPossibleChars;

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintInfo {
		private char hintChar;
		private ArrayList<String> IDList = new ArrayList<String>();
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PasswordInfo {
		private String passwordHash;
		private String crackedPassword;
		private ArrayList<Character> possibleChars = new ArrayList<Character>();
	}

	private HashMap<String, HintInfo> hints = new HashMap<String, HintInfo>();
	// for passwords: String[] = {passwordHash, crackedPassword, possibleChars}
	private HashMap<String, PasswordInfo> passwords = new HashMap<String, PasswordInfo>();

	private HashMap<String, Boolean> searchedChars = new HashMap<String, Boolean>();
	private ArrayList<ArrayList<Character>> charCombinationsToSearch = new ArrayList<ArrayList<Character>>();
	
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

		if (lines.isEmpty()) {
			// 1. for every possible password char:
			//		send Worker message with (char, hints)
			this.distributeWork();
			System.out.println("lines is empty");
			//this.collector.tell(new Collector.PrintMessage(), this.self());
			//this.terminate();
			return;
		}

		String[] firstLine = lines.get(0);

		if(possiblePasswordChars.isEmpty()) {
			for (char possibleChar : firstLine[2].toCharArray()) {
				possiblePasswordChars.add(new Character(possibleChar));
			}

			numberOfPossiblePasswordChars = possiblePasswordChars.size();
			// int numberOfPossibleHints = factorial(numberOfPossiblePasswordChars - 1);
			passwordLength = Integer.parseInt(firstLine[3]); // get password length
			// int numberOfPossiblePasswords = numberOfPossiblePasswordChars ** passwordLength;
			numberOfHints = firstLine.length - 5; // get number of hints
			// calculate minPossibleChars before crack password (maxNumberOfHints < verfügbare Hints)
			minPossibleChars = 2;

			for (Character charToBeLeftOut : possiblePasswordChars) {
				ArrayList<Character> charsToSearch = new ArrayList<Character>();
				for (Character charToBeAdded : possiblePasswordChars) {
					if(charToBeLeftOut != charToBeAdded) {
						charsToSearch.add(charToBeAdded);
					}
				}
				charCombinationsToSearch.add(charsToSearch);
			}

			System.out.println(possiblePasswordChars);
		}

		for (String[] line : lines) {
			// create 1. hints with all hints, and ArrayList with crackedHint (empty at start) and List of line-IDs !!!
			for (int hintNr = 4; hintNr < (numberOfHints + 4); hintNr++) {
				if(!hints.containsKey(line[hintNr])) {
					ArrayList<String> IDList = new ArrayList<String>();
					IDList.add(line[0]);
					HintInfo crackedHintPlusIDs = new HintInfo(Character.MIN_VALUE, IDList);
					hints.put(line[hintNr], crackedHintPlusIDs);
				} else {
					HintInfo crackedHintPlusIDs = hints.get(line[hintNr]);
					crackedHintPlusIDs.getIDList().add(line[0]);
					hints.put(line[hintNr], crackedHintPlusIDs);
				}
			}

			// create 2. hashmap with line-IDs, password-hash, possible chars
			PasswordInfo passwordInfo = new PasswordInfo(line[4], null, new ArrayList<>(possiblePasswordChars));
			passwords.put(line[0], passwordInfo);
		}
		
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
		System.out.println(charCombinationsToSearch);
		char[] charsToSearch = getCharArrayFromArrayList(charCombinationsToSearch.remove(0));
		worker.tell(new Worker.SolveHintsMessage(charsToSearch), this.self());
	}

	protected char[] getCharArrayFromArrayList(ArrayList<Character> arrayList) {
		char[] charArray = new char[arrayList.size()];
		Character[] characterArray = arrayList.toArray(new Character[arrayList.size()]);
		for (int i = 0; i < arrayList.size(); i++) {
			charArray[i] = characterArray[i].charValue();
		}
		return charArray;
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

		System.out.println("Solved hints: " + solvedHints.size());
		
		for(String[] solvedHint : solvedHints) {
			HintInfo hint = hints.get(solvedHint[0]);
			System.out.println("Solved hint: " + solvedHint[1]);
			Character solvedHintChar = this.getSolvedHintChar(solvedHint[1]); //will be the same for all because its just permutations, could be better (only send line and letter?)
			hint.setHintChar(solvedHintChar);
			System.out.println("Solved char: " + solvedHintChar);
			hints.put(solvedHint[0], hint);
			ArrayList<String> affectedPasswordLines = hint.getIDList();
			for(String line : affectedPasswordLines) {
				System.out.println("line: " + line);
				PasswordInfo passwordInfo = passwords.get(line);
				if(passwordInfo.getPossibleChars().contains(solvedHintChar)) {
					passwordInfo.getPossibleChars().remove(solvedHintChar);
				}
				System.out.println("remaining chars: " + passwordInfo.getPossibleChars());
				ArrayList<Character> possibleChars = passwordInfo.getPossibleChars();
				passwordInfo.setPossibleChars(possibleChars);
				if(possibleChars.size() <= minPossibleChars && passwordInfo.getCrackedPassword() == null) {
					System.out.println("Crack pw");
					char[] possibleCharsArray = getCharArrayFromArrayList(possibleChars);
					worker.tell(new Worker.CrackPasswordMessage(line, passwordInfo.getPasswordHash(), possibleCharsArray), this.self());
				} else {
					// System.out.println("Solve more");
					// this.sendWork(worker);
				}
			}
		}
	}

	protected Character getSolvedHintChar(String solvedHint) {
		for(char possibleChar : possiblePasswordChars) {
			if (!solvedHint.contains(Character.toString(possibleChar))) {
				return (Character) possibleChar;
			}
		}
		return (Character) Character.MIN_VALUE;
	}

	protected void handle(PasswordCrackedMessage message) {
		String lineID = message.getLineID();
		PasswordInfo passwordInfo = passwords.get(lineID);
		passwordInfo.setCrackedPassword(message.getCrackedPassword());
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
