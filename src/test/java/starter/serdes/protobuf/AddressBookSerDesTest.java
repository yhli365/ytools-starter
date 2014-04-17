package starter.serdes.protobuf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.junit.Test;

import starter.Constant;
import example.protobuf.tutorial.AddressBookProtos.AddressBook;
import example.protobuf.tutorial.AddressBookProtos.Person;
import example.protobuf.tutorial.AddressBookProtos2.AddressBook2;
import example.protobuf.tutorial.AddressBookProtos2.Person2;

public class AddressBookSerDesTest {

	private File file = new File(Constant.TEMP_DIR + "persons.pbf");

	@Test
	public void addPerson() throws IOException {
		AddressBook.Builder addressBook = AddressBook.newBuilder();

		// Read the existing address book.
		if (file.exists()) {
			FileInputStream input = new FileInputStream(file);
			try {
				addressBook.mergeFrom(input);
			} finally {
				try {
					input.close();
				} catch (Throwable ignore) {
				}
			}
		}

		// Add an address.
		addressBook.addPerson(createPerson());

		// Write the new address book back to disk.
		FileOutputStream output = new FileOutputStream(file);
		try {
			addressBook.build().writeTo(output);
		} finally {
			output.close();
		}
	}

	@Test
	public void listPerson() throws IOException {
		if (!file.exists()) {
			return;
		}
		// Read the existing address book.
		AddressBook addressBook = AddressBook.parseFrom(new FileInputStream(
				file));

		print(addressBook);
	}

	/**
	 * @throws IOException
	 */
	@Test
	public void listPerson2() throws IOException {
		if (!file.exists()) {
			return;
		}
		// Read the existing address book.
		AddressBook2 addressBook = AddressBook2.parseFrom(new FileInputStream(
				file));

		for (Person2 person : addressBook.getPersonList()) {
			System.out.println("Person ID: " + person.getId());
			System.out.println("  Name: " + person.getName());
			if (person.hasEmail()) {
				System.out.println("  E-mail address: " + person.getEmail());
			}

			for (Person2.PhoneNumber phoneNumber : person.getPhoneList()) {
				switch (phoneNumber.getType()) {
				case MOBILE:
					System.out.print("  Mobile phone #: ");
					break;
				case HOME:
					System.out.print("  Home phone #: ");
					break;
				case WORK:
					System.out.print("  Work phone #: ");
					break;
				}
				System.out.println(phoneNumber.getNumber());
			}
			System.out.println(" +Desc: " + person.getDesc());
		}
	}

	private Person createPerson() {
		long ts = System.currentTimeMillis();
		String tag = new SimpleDateFormat("yyyyMMddHHmmss")
				.format(new Date(ts));
		Person.Builder person = Person.newBuilder();
		int pid = (int) ts / 1000;
		if (pid < 0) {
			pid = -pid;
		}
		person.setId(pid);
		person.setName("name-" + tag);

		Random rnd = new Random();
		int d = rnd.nextInt(10);
		if (d % 2 == 0) {
			person.setEmail("email-" + d + "@111.com");
		}

		for (int i = 0; i < d % 3; i++) {
			int d2 = rnd.nextInt(3);

			Person.PhoneNumber.Builder phoneNumber = Person.PhoneNumber
					.newBuilder().setNumber("123456" + d + d2);
			if (d2 == 0) {
				phoneNumber.setType(Person.PhoneType.MOBILE);
			}
			if (d2 == 1) {
				phoneNumber.setType(Person.PhoneType.HOME);
			} else {
				phoneNumber.setType(Person.PhoneType.WORK);
			}
			person.addPhone(phoneNumber);
		}

		return person.build();
	}

	// Iterates though all people in the AddressBook and prints info about them.
	private void print(AddressBook addressBook) {
		for (Person person : addressBook.getPersonList()) {
			System.out.println("Person ID: " + person.getId());
			System.out.println("  Name: " + person.getName());
			if (person.hasEmail()) {
				System.out.println("  E-mail address: " + person.getEmail());
			}

			for (Person.PhoneNumber phoneNumber : person.getPhoneList()) {
				switch (phoneNumber.getType()) {
				case MOBILE:
					System.out.print("  Mobile phone #: ");
					break;
				case HOME:
					System.out.print("  Home phone #: ");
					break;
				case WORK:
					System.out.print("  Work phone #: ");
					break;
				}
				System.out.println(phoneNumber.getNumber());
			}
		}
	}

}
