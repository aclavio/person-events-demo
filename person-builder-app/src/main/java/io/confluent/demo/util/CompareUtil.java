package io.confluent.demo.util;

import io.confluent.demo.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompareUtil {

    private static final Logger logger = LoggerFactory.getLogger(CompareUtil.class);

    public static int computePersonHash(Person person) {
        int hash = 7;
        hash = 31 * hash + (person.getFirstName() == null ? 0 : person.getFirstName().hashCode());
        hash = 31 * hash + (person.getMiddleName() == null ? 0 : person.getMiddleName().hashCode());
        hash = 31 * hash + (person.getLastName() == null ? 0 : person.getLastName().hashCode());
        hash = 31 * hash + (person.getGender() == null ? 0 : person.getGender().hashCode());
        hash = 31 * hash + (person.getSsn() == null ? 0 : person.getSsn().hashCode());
        hash = 31 * hash + (person.getDateOfBirth() == null ? 0 : person.getDateOfBirth().hashCode());
        hash = 31 * hash + (person.getDateOfDeath() == null ? 0 : person.getDateOfDeath().hashCode());
        hash = 31 * hash + (person.getBirthLocationName() == null ? 0 : person.getBirthLocationName().hashCode());
        hash = 31 * hash + (person.getBirthLocationCountry() == null ? 0 : person.getBirthLocationCountry().hashCode());
        hash = 31 * hash + (person.getBirthLocationState() == null ? 0 : person.getBirthLocationState().hashCode());
        hash = 31 * hash + (person.getBirthLocationZipcode() == null ? 0 : person.getBirthLocationZipcode().hashCode());
        logger.debug("computePersonHash() called - Computed hash: {}", hash);
        return hash;
    }
}
