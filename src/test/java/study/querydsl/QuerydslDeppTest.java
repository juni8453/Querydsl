package study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static study.querydsl.entity.QMember.member;

@Transactional
@SpringBootTest
public class QuerydslDeppTest {

    @PersistenceContext
    EntityManager em;

    // 멀티쓰레드 Safe 이기 때문에 필드타입으로 빼도 안전하다.
    JPAQueryFactory queryFactory;

    @BeforeEach
    public void before() {
        queryFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member memberA = new Member("memberA", 10, teamA);
        Member memberB = new Member("memberB", 20, teamA);
        Member memberC = new Member("memberC", 30, teamB);
        Member memberD = new Member("memberD", 40, teamB);
        em.persist(memberA);
        em.persist(memberB);
        em.persist(memberC);
        em.persist(memberD);

        em.flush();
        em.clear();
    }

    @Test
    void simpleProjection() {

        // 프로젝션 대상 하나는 타입이 정해져 있어서 심플하다.
        List<String> result1 = queryFactory
            .select(member.username)
            .from(member)
            .fetch();

        List<Member> result2 = queryFactory
            .select(member)
            .from(member)
            .fetch();
    }

    @Test
    void tupleProjection() {

        // 프로젝션 대상의 타입이 일치하지 않는 경우 Tuple 타입을 사용한다.
        // Tuple 은 Querydsl.core 라이브러리 내부에 있기 때문에 종속적 즉, Repository 계층 이외로 넘어가는 것은 좋지않다.
        // TODO 따라서 바깥 계층으로 넘길 때는 DTO 를 사용하자.
        List<Tuple> result = queryFactory
            .select(member.username, member.age)
            .from(member)
            .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple.get(member.username));
            System.out.println("tuple = " + tuple.get(member.age));
        }
    }

    // 순수 JPA 에서 DTO 반환하기
    // JPQL 에서 지원하는 new Operation 문법으로 Select 문에서 생성자를 직접 만드는 식으로 타입을 맞춰줄 수는 있다.
    @Test
    void findDtoByJPQL() {
        List<MemberDto> resultDto = em.createQuery("select " +
                " new study.querydsl.dto.MemberDto(m.username, m.age) " +
                " from Member m", MemberDto.class)
            .getResultList();

        for (MemberDto memberDto : resultDto) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    // Querydsl 에서 DTO 반환하기 1
    // DTO 에 Getter, Setter 가 필요하다.
    @Test
    void findDtoBySetter() {
        List<MemberDto> result = queryFactory
            .select(Projections.bean(MemberDto.class,
                member.username,
                member.age))
            .from(member)
            .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    // Querydsl 에서 DTO 반환하기 2
    // Getter, Setter 필요없이 Member 에서 조회한 값을 DTO 필드 값에 바로 꽂아준다. (리플렉션 등으로)
    @Test
    void findDtoByField() {
        List<MemberDto> result = queryFactory
            .select(Projections.fields(MemberDto.class,
                member.username,
                member.age))
            .from(member)
            .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    // Querydsl 에서 DTO 반환하기 3
    // 생성자를 활용해 DTO 반환
    @Test
    void findDtoByConstructor() {
        List<MemberDto> fetch = queryFactory
            .select(Projections.constructor(MemberDto.class,
                member.username,
                member.age))
            .from(member)
            .fetch();

        for (MemberDto memberDto : fetch) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    // fields() 를 사용해 DTO 를 반환할 때, DTO 필드 명과 Entity 필드 명이 다른 경우 alias 를 붙여서 필드 명을 맞춰줘야 한다.
    // bean()) 을 사용할 떄도 마찬가지.
    // 예외는 터지지 않지만 매칭이 안된 필드에 null 값이 들어간다.
    // 현재 Member 에서는 username 이지만 UserDto 는 name 이기 때문에 alias 를 맞춰주면 됨.
    @Test
    void findUserDtoByField() {
        List<UserDto> fetch = queryFactory
            .select(Projections.fields(UserDto.class,
                member.username.as("name"),
                member.age))
            .from(member)
            .fetch();

        for (UserDto userDto : fetch) {
            System.out.println("userDto = " + userDto);
        }
    }

    // DTO 반환 시 서브쿼리를 사용하고 싶다면 ExpressionUtils 를 사용해 Alias 를 줘서 필드명을 맞춰주면 된다.
    // 2번째 인자로 Alias 를 지정할 수 있기 때문!
    @Test
    void findUserDtoUseSubQuery() {
        QMember memberSub = new QMember("memberSub");

        List<UserDto> fetch = queryFactory
            .select(Projections.fields(UserDto.class,
                member.username.as("name"), // 여기서도 ExpressionUtils 사용해도 된다.

                ExpressionUtils.as(JPAExpressions
                    .select(memberSub.age.max())
                    .from(memberSub), "age")
            ))
            .from(member)
            .fetch();

        for (UserDto userDto : fetch) {
            System.out.println("userDto = " + userDto);
        }
    }

    @Test
    void findUserDtoConstructor() {
        List<UserDto> fetch = queryFactory
            .select(Projections.constructor(UserDto.class,
                member.username,
                member.age))
            .from(member)
            .fetch();

        for (UserDto userDto : fetch) {
            System.out.println("userDto = " + userDto);
        }
    }


    // DTO 생성자에 @QueryProjection 을 사용해 Q 타입 DTO 를 생성해 바로 반환받을 수 있다.
    // Projections.constructor 에서는 잡아내지 못하는 컴파일시 오류를 잡아낼 수 있다. (생성자 인자를 잘못넣는 등)
    // 다만 Q 타입 파일을 생성해야하고, 해당 DTO 가 Querydsl.core 라이브러리에 대한 의존이 생긴다.
    // DTO 는 여러 레이어를 타면서 사용되는데 순수하지 못한 DTO 를 사용하는 것에서 약간 고민거리가 생길 수 있다.
    @Test
    void findDtoByQueryProjection() {
        List<MemberDto> result = queryFactory
            .select(new QMemberDto(member.username, member.age))
            .from(member)
            .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    // 동적쿼리 BooleanBuilder 사용
    // BooleanBuilder 의 메서드를 사용해 여러 조건을 주고 where 절에 builder 를 넣으면 된다.
    @Test
    void useBooleanBuilder() {
        String usernameParam = "memberA";
        Integer ageParam = null;

        List<Member> result = searchMemberA(usernameParam, ageParam);
        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMemberA(String usernameCond, Integer ageCond) {

        // 생성자 인자로 무조건 있어야 하는 값을 넣어 초기 값을 셋팅할 수도 있다.
        BooleanBuilder booleanBuilder = new BooleanBuilder();
        if (usernameCond != null) {
            booleanBuilder.and(member.username.eq(usernameCond));
        }

        if (ageCond != null) {
            booleanBuilder.and(member.age.eq(ageCond));
        }

        return queryFactory
            .select(member)
            .from(member)
            .where(booleanBuilder) // 조건을 더 붙일 수 있다.
            .fetch();
    }

    // 동적쿼리 where 다중쿼리 사용
    // where 절에 메서드 리턴 값을 받아 바로 처리하는 방법
    // 여러 개의 메서드를 하나의 메서드로 조립도 가능하며 메서드를 다른 쿼리에서 재활용 할 수 있다.
    // TODO null 체크 잘 하면서 사용하자.
    @Test
    void useWhere() {
        String usernameParam = "memberA";
        Integer ageParam = null;

        List<Member> result = searchMemberB(usernameParam, ageParam);
        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMemberB(String usernameCond, Integer ageCond) {
        return queryFactory
            .select(member)
            .from(member)
//            .where(usernameEq(usernameCond), ageEq(ageCond))
            .where(allEq(usernameCond, ageCond))
            .fetch();
    }

    private BooleanExpression allEq(String usernameCond, Integer ageCond) {
        return usernameEq(usernameCond).and(ageEq(ageCond));
    }

    private BooleanExpression usernameEq(String usernameCond) {
        if (usernameCond == null) {
            return null;

        }
        return member.username.eq(usernameCond);
    }

    private BooleanExpression ageEq(Integer ageCond) {
        if (ageCond == null) {
            return null;

        }
        return member.age.eq(ageCond);
    }

    // 벌크 연산은 영속성 컨텍스트를 무시하고 DB 에 바로 쿼리를 날리기 때문에 잘못하면 영속성 컨텍스트와 DB 값이 달라질 수 있다.
    @Test
    void bulkUpdate() {
        // count = 영향을 받는 Row 수
        /** update 후
         * member1 = 10 -> (영속성) 10 / DB 비회원
         * member2 = 20 -> (영속성) 20 / DB 비회원
         * member3 = 30 -> 30 / DB 30
         * member4 = 40 -> 40 / DB 30
         */
        long count = queryFactory
            .update(member)
            .set(member.username, "비회원")
            .where(member.age.lt(28))
            .execute();

        // TODO 해결방안
        //  벌크 연산 시 이미 DB 와 영속성 컨텍스트가 맞지 않기 때문에 영속성 컨텍스트를 비워주면 된다.
        //  Spring Data JPA 의 경우 @Modifying 과 같은 어노테이션으로 지원한다.
        em.flush();
        em.clear();

        /** 다시 조회하면 ?
         * DB 에서 값을 가져와도 영속성 컨텍스트가 우선권을 가지기 때문에 member1, member2 값이 10, 20 으로 조회된다.
         * 영속성 컨텍스트에 member1 = 10, member2 = 20 값이 남아있으니까 !
         */
        List<Member> result = queryFactory
            .selectFrom(member)
            .fetch();

        for (Member member1 : result) {
            System.out.println("member1 = " + member1);
        }
    }

    // 모든 회원에 나이를 하나 더하기. 뺄 때는 add(-1) 로 음수를 주면 된다.
    @Test
    void bulkAdd() {
        long count = queryFactory
            .update(member)
            .set(member.age, member.age.add(1))
            .execute();
    }

    // 18 살 이상의 모든 회원을 지우기.
    @Test
    void bulkDelete() {
        long count = queryFactory
            .delete(member)
            .where(member.age.gt(18))
            .execute();
    }

    // SQL 함수 실행하기 (replace)
    // 간단한 SQL 함수는 내장이기 때문에 사용가능하다.
    @Test
    void sqlFunc() {
        List<String> result = queryFactory
            .select(Expressions.stringTemplate(
                "function('replace', {0}, {1}, {2})",
                member.username, "member", "M"))
            .from(member)
            .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    // SQL 함수 실행하기 (소문자로 바꾸기)
    @Test
    void sqlFunc2() {
        List<String> result = queryFactory
            .select(member.username)
            .from(member)
//            .where(member.username.eq(
//                Expressions.stringTemplate(
//                    "function('lower', {0}",
//                    member.username)))
            .where(member.username.eq(member.username.lower())) // 내장함수가 있으면 그걸 사용
         .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }
}