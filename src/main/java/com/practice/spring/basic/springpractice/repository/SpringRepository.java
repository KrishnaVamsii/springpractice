package com.practice.spring.basic.springpractice.repository;

import com.practice.spring.basic.springpractice.model.UserRoles;
import org.h2.engine.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface SpringRepository extends JpaRepository<UserRoles,Integer> {


    @Query("from UserRoles WHERE status='ACTIVE' AND roleName='TestRole' ")
    UserRoles getUserRole();

}
