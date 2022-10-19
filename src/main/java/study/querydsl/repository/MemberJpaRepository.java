package study.querydsl.repository;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.jpa.impl.JPAQueryFactory;

import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;
import study.querydsl.dto.MemberSearchCondition;
import study.querydsl.dto.MemberTeamDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.QMemberTeamDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;

import javax.persistence.EntityManager;
import java.util.List;
import java.util.Optional;

import static org.springframework.util.StringUtils.*;
import static study.querydsl.entity.QMember.*;
import static study.querydsl.entity.QTeam.*;

@Repository
public class MemberJpaRepository {

    private final EntityManager em;
    private final JPAQueryFactory queryFactory;

    public MemberJpaRepository(EntityManager em) {
        this.em = em;
        this.queryFactory = new JPAQueryFactory(em);
    }

    public void save(Member member) {
        em.persist(member);
    }

    public Optional<Member> findById(Long id) {
        Member findMember = em.find(Member.class, id);
        return Optional.ofNullable(findMember);
    }

    public List<Member> findAll() {
        return em.createQuery("select m from Member m", Member.class)
            .getResultList();
    }

    public List<Member> findAll_Querydsl() {
        return queryFactory
            .selectFrom(member)
            .fetch();
    }

    public List<Member> findAllByUsername(String username) {
        return em.createQuery("select m from Member m where m.username = :username", Member.class)
            .setParameter("username", username)
            .getResultList();
    }

    public List<Member> findAllByUsername_Querydsl(String username) {
        return queryFactory
            .select(member)
            .from(member)
            .where(member.username.eq(username))
            .fetch();
    }

    // 동적쿼리로 검색 (Builder 사용)
    public List<MemberTeamDto> searchByBuilder(MemberSearchCondition condition) {

        BooleanBuilder booleanBuilder = new BooleanBuilder();
        if (hasText(condition.getUsername())) { // null, "" 한꺼번에 처리
            booleanBuilder.and(member.username.eq(condition.getUsername()));
        }

        if (hasText(condition.getTeamName())) {
            booleanBuilder.and(team.name.eq(condition.getTeamName()));
        }

        if (condition.getAgeGoe() != null) {
            booleanBuilder.and(member.age.goe(condition.getAgeGoe()));
        }

        if (condition.getAgeLoe() != null) {
            booleanBuilder.and(member.age.loe(condition.getAgeLoe()));
        }

        return queryFactory
            .select(new QMemberTeamDto(
                member.id.as("memberId"), // Member 엔티티 필드명을 MemberTeamDTO 필드명과 맞춰주기
                member.username,
                member.age,
                team.id.as("teamId"), // Team 엔티티 필드명을 MemberTeamDTO 필드명과 맞춰주기
                team.name.as("teamName") // Team 엔티티 필드명을 MemberTeamDTO 필드명과 맞춰주기
            ))
            .from(member)
            .leftJoin(member.team, team)
            .where(booleanBuilder)
            .fetch();
    }

    // 동적쿼리로 검색 (Where Param 사용)
    public List<MemberTeamDto> searchByWhereParam(MemberSearchCondition condition) {
        return queryFactory
            .select(new QMemberTeamDto(
                member.id.as("memberId"),
                member.username,
                member.age,
                team.id.as("teamId"),
                team.name.as("teamName")
            ))
            .from(member)
            .leftJoin(member.team, team)
            .where(
                // 콤마로 연결하면 and() 라는 뜻
                usernameEq(condition.getUsername()),
                teamNameEq(condition.getTeamName()),
                ageGoe(condition.getAgeGoe()),
                ageLoe(condition.getAgeLoe()))
            .fetch();
    }

    private BooleanExpression usernameEq(String username) {
        if (!hasText(username)) {
            return null;
        }

        return member.username.eq(username);
    }

    private BooleanExpression teamNameEq(String teamName) {
        if (!hasText(teamName)) {
            return null;
        }

        return team.name.eq(teamName);
    }

    private BooleanExpression ageGoe(Integer ageGoe) {
        if (ageGoe == null) {
            return null;
        }

        return member.age.goe(ageGoe);
    }

    private BooleanExpression ageLoe(Integer ageLoe) {
        if (ageLoe == null) {
            return null;
        }

        return member.age.loe(ageLoe);
    }
}
