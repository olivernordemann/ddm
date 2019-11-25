package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import static java.lang.Thread.sleep;


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
		this.workers = new ArrayList<ActorRef>();
		this.idleWorkers = new ArrayDeque<ActorRef>();
		this.workStack = new ArrayDeque<Object>();
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
		private static final long serialVersionUID = 7763040942748609599L;
		private ArrayList<String[]> solvedHints;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PasswordCrackedMessage implements Serializable {
		private static final long serialVersionUID = 7763040942748609100L;
		private Integer lineID;
		private String crackedPassword;
	}

	@Data
	public static class PasswordsPrintedMessage implements Serializable {
		private static final long serialVersionUID = 7763040942748609111L;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final Deque<ActorRef> idleWorkers;
	private Deque<Object> workStack;


	private long startTime;

	private Worker.HintsMessage hintsMessage;
	private Worker.PasswordLengthMessage passwordLengthMessage;
	private ArrayList<Character> possiblePasswordChars = new ArrayList<Character>();
	private int numberOfPossiblePasswordChars;
	private int passwordLength;
	private int numberOfHints;
	private int minPossibleChars;
	private int numberOfPasswords;

	private HashMap<String, HintInfo> hints = new HashMap<String, HintInfo>();
	private ArrayList<PasswordInfo> passwords = new ArrayList<PasswordInfo>();
	private HashMap<String, Boolean> searchedChars = new HashMap<String, Boolean>();
	private ArrayList<ArrayList<Character>> charCombinationsToSearch = new ArrayList<ArrayList<Character>>();
	private Set<Integer> crackedLineIDs = new HashSet<>();
	private HashMap<ActorRef, Object> workingOn = new HashMap<ActorRef, Object>();

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintInfo {
		private char hintChar;
		private ArrayList<Integer> IDList = new ArrayList<Integer>();
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PasswordInfo {
		private String passwordHash;
		private String crackedPassword;
		private ArrayList<Character> possibleChars = new ArrayList<Character>();
		private int nrHintsSolved;
	}


	
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
				.match(PasswordCrackedMessage.class, this::handle)
				.match(PasswordsPrintedMessage.class, this::handle)
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
			this.numberOfPasswords = passwords.size();
			this.distributeWork();
			return;
		}

		if(possiblePasswordChars.isEmpty()) { //initialize state
			String[] firstLine = lines.get(0);

			for (char possibleChar : firstLine[2].toCharArray()) {
				possiblePasswordChars.add(new Character(possibleChar));
			}
			numberOfPossiblePasswordChars = possiblePasswordChars.size();
			passwordLength = Integer.parseInt(firstLine[3]);
			numberOfHints = firstLine.length - 5;
			minPossibleChars = passwordLength - numberOfHints + 1; // first solve ALL hints, then start cracking
			if(minPossibleChars<2) {
				minPossibleChars = 2;
			}

			for (Character charToBeLeftOut : possiblePasswordChars) {
				ArrayList<Character> charsToSearch = new ArrayList<Character>();
				for (Character charToBeAdded : possiblePasswordChars) {
					if(charToBeLeftOut != charToBeAdded) {
						charsToSearch.add(charToBeAdded);
					}
				}
				charCombinationsToSearch.add(charsToSearch);
			}
		}

		int numberOfExistingPasswords = passwords.size();
		int numberofLinesInBatch = lines.size();

		for (int lineSequenceNumber = 0; lineSequenceNumber < numberofLinesInBatch; lineSequenceNumber++) {
			Integer lineID = numberOfExistingPasswords + lineSequenceNumber;
			String[] line = lines.get(lineSequenceNumber);
			for (int hintNr = 5; hintNr < (numberOfHints + 5); hintNr++) {
				if(!hints.containsKey(line[hintNr])) {
					ArrayList<Integer> IDList = new ArrayList<Integer>();
					IDList.add(lineID);
					HintInfo crackedHintPlusIDs = new HintInfo(Character.MIN_VALUE, IDList);
					hints.put(line[hintNr], crackedHintPlusIDs);
				} else {
					HintInfo crackedHintPlusIDs = hints.get(line[hintNr]);
					crackedHintPlusIDs.getIDList().add(lineID);
					hints.put(line[hintNr], crackedHintPlusIDs);
				}
			}
			PasswordInfo passwordInfo = new PasswordInfo(line[4], null, new ArrayList<>(possiblePasswordChars), 0);
			passwords.add(passwordInfo);
		}
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	protected void distributeWork() {
		// start the workers which are already registered, send one message to every worker
		hintsMessage = new Worker.HintsMessage(hints);
		passwordLengthMessage = new Worker.PasswordLengthMessage(passwordLength);
		// verteile Infos zum Lösen der Hints und Passwörter auf alle bereits registrierten worker
		for (ActorRef worker : workers) {
			worker.tell(hintsMessage, this.self());
			worker.tell(passwordLengthMessage, this.self());
		}
		// push SolveHintMessages with char combinations on workStack
		while(!charCombinationsToSearch.isEmpty()) {
			char[] charsToSearch = getCharArrayFromArrayList(charCombinationsToSearch.remove(0));
			assign(new Worker.SolveHintsMessage(charsToSearch));
		}
	}

	protected char[] getCharArrayFromArrayList(ArrayList<Character> arrayList) {
		char[] charArray = new char[arrayList.size()];
		Character[] characterArray = arrayList.toArray(new Character[arrayList.size()]);
		for (int i = 0; i < arrayList.size(); i++) {
			charArray[i] = characterArray[i].charValue();
		}
		return charArray;
	}
	
	protected void terminate()  {

		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);

		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), this.getSelf());
		}
		this.reader.tell(PoisonPill.getInstance(), this.getSelf());
		this.collector.tell(PoisonPill.getInstance(), this.getSelf());

		try{ Thread.sleep(500); } catch(InterruptedException e){}
		this.getSelf().tell(PoisonPill.getInstance(), this.getSelf());
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());

		// if information messages for solving work are ready, then send them to worker
		if (hintsMessage != null) { this.sender().tell(hintsMessage, this.self()); }
		if (passwordLengthMessage != null) { this.sender().tell(passwordLengthMessage, this.self()); }

		assign(this.sender());
	}

	protected void handle(HintsSolvedMessage message) {
		ActorRef worker = this.getSender();
		ArrayList<String[]> solvedHints = message.getSolvedHints();

		Character solvedHintChar = Character.MIN_VALUE;
		if(solvedHints.size() > 0) {
			solvedHintChar = this.getSolvedHintChar(solvedHints.get(0)[1]); // worker solved immer genau einen buchstaben
		}

		for(String[] solvedHint : solvedHints) {
			HintInfo hint = hints.get(solvedHint[0]); // sucht in hint hashmap den Hash des gelösten Hinweises
			hint.setHintChar(solvedHintChar);
			hints.put(solvedHint[0], hint);
			ArrayList<Integer> affectedPasswordLines = hint.getIDList();
			for(Integer lineID : affectedPasswordLines) {
				PasswordInfo passwordInfo = passwords.get(lineID);
				passwordInfo.setNrHintsSolved(passwordInfo.getNrHintsSolved()+1);
				ArrayList<Character> possibleChars = passwordInfo.getPossibleChars();
				if(possibleChars.contains(solvedHintChar)) {
					possibleChars.remove(solvedHintChar);
					passwordInfo.setPossibleChars(possibleChars);
				}
				if(possibleChars.size() <= minPossibleChars || passwordInfo.getNrHintsSolved() >= this.numberOfHints) {
					char[] possibleCharsArray = getCharArrayFromArrayList(passwordInfo.getPossibleChars());
					workStack.push(new Worker.CrackPasswordMessage(lineID, passwordInfo.getPasswordHash(), possibleCharsArray));
				}
			}
		}

		assign(worker);
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
		ActorRef worker = this.getSender();
		Integer lineID = message.getLineID();
		PasswordInfo passwordInfo = passwords.get(lineID);
		passwordInfo.setCrackedPassword(message.getCrackedPassword());
		passwords.set(lineID, passwordInfo);
		this.crackedLineIDs.add(lineID);
		
		if(this.numberOfPasswords == this.crackedLineIDs.size()) {
			endWork();
		} else {
			assign(worker);
		}
	}
	protected void handle(PasswordsPrintedMessage message) {
		terminate();
	}

	protected void endWork() {

		for (Integer lineID = 0; lineID < passwords.size(); lineID++ ) {
			PasswordInfo passwordInfo = passwords.get(lineID);
			this.collector.tell(new Collector.CollectMessage(passwordInfo.getCrackedPassword()), this.self());
		}
		this.collector.tell(new Collector.PrintMessage(), this.self());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		workers.remove(message.getActor());
		idleWorkers.remove(message.getActor());
		assign(workingOn.get(message.getActor()));
	}

	private void assign(Object work) {
		ActorRef worker = idleWorkers.poll();

		if (worker == null) {
			workStack.push(work);
			return;
		}
		worker.tell(work, this.self());
		workingOn.put(worker, work);
	}

	private void assign(ActorRef worker) {
		Object work = workStack.poll();

		if (work == null) {
			idleWorkers.add(worker);
			return;
		}
		worker.tell(work, this.self());
		workingOn.put(worker, work);
	}
}
