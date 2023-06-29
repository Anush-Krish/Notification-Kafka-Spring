package net.javaguides.springboot.repository;

import net.javaguides.springboot.entity.WikimediaData;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WikimediaDataRepo extends JpaRepository<WikimediaData,Long> {

}
