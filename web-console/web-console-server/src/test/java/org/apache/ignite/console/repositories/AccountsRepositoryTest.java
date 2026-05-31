

package org.apache.ignite.console.repositories;

import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.console.TestGridConfiguration;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.apache.ignite.console.utils.TestUtils.cleanPersistenceDir;
import static org.apache.ignite.console.utils.TestUtils.stopAllGrids;

/**
 * Accounts repository test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestGridConfiguration.class)
public class AccountsRepositoryTest {
    /** Accounts repository. */
    @Autowired
    private AccountsRepository accountsRepo;

    /**
     * @throws Exception If failed.
     */
    @BeforeClass
    public static void setup() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @AfterClass
    public static void tearDown() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * Should throw account not found.
     */
    @Test
    public void testAccountNotFoundExceptionDuringDelete() {
        UUID accId = UUID.randomUUID();
        GridTestUtils.assertThrows(null, () -> {
            accountsRepo.delete(accId);
            return null;
        }, IllegalStateException.class, "Account not found for ID: " + accId);
    }

    /**
     * Should throw account already registered exception.
     */
    @Test
    public void testAccountExistByEmailDuringSave() {
        UUID accId = UUID.randomUUID();
        Account acc = new Account(
            "mail@mail",
            "pswd",
            "name",
            "lastName",
            "phone",
            "company",
            "country"
        );
        acc.setId(accId);

        GridTestUtils.assertThrows(null, () -> {
            accountsRepo.save(acc);
            acc.setId(UUID.randomUUID());
            accountsRepo.save(acc);

            return null;
        }, IgniteException.class, "The email address you have entered is already registered: " + acc.getUsername());
    }

    /**
     * Should throw account already exists exception.
     */
    @Test
    public void testAccountExistByTokenDuringSave() {
        UUID accId = UUID.randomUUID();
        Account acc = new Account(
                "mail1@mail",
                "pswd",
                "name",
                "lastName",
                "phone",
                "company",
                "country"
        );
        acc.setId(accId);
        acc.setToken("token");

        GridTestUtils.assertThrows(null, () -> {
            accountsRepo.save(acc);
            acc.setId(UUID.randomUUID());
            acc.setEmail("mail2@mail");
            accountsRepo.save(acc);

            return null;
        }, IgniteException.class, "Account with token " + acc.getToken() + " already exists");
    }
}
