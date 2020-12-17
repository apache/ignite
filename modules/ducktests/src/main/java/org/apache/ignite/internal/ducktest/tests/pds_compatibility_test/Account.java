package org.apache.ignite.internal.ducktest.tests.pds_compatibility_test;

import java.io.Serializable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Account implements Serializable {

    @QuerySqlField(index = true, inlineSize = 48)
    private final String firstName;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String lastName;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String catName;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String dogName;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String city;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String country;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String eMail;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String phoneNumber;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String socialNumber;

    @QuerySqlField(index = true, inlineSize = 48)
    private final Long postIndex;

    public final long balance;

    public Account(String firstName, String lastName, String catName, String dogName, String city, String country, String eMail, String phoneNumber, String socialNumber, Long postIndex, long balance) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.catName = catName;
        this.dogName = dogName;
        this.city = city;
        this.country = country;
        this.eMail = eMail;
        this.phoneNumber = phoneNumber;
        this.socialNumber = socialNumber;
        this.postIndex = postIndex;
        this.balance = balance;
    }

    @Override
    public String toString() {
        return "Account{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", catName='" + catName + '\'' +
                ", dogName='" + dogName + '\'' +
                ", city='" + city + '\'' +
                ", country='" + country + '\'' +
                ", eMail='" + eMail + '\'' +
                ", phoneNumber='" + phoneNumber + '\'' +
                ", socialNumber='" + socialNumber + '\'' +
                ", postIndex='" + postIndex + '\'' +
                ", balance=" + balance +
                '}';
    }
}
