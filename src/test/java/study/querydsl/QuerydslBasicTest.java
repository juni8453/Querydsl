package study.querydsl;

import com.querydsl.core.Tuple;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.impl.JPAQueryFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;

import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceUnit;

import java.util.List;

import static com.querydsl.jpa.JPAExpressions.*;
import static org.assertj.core.api.Assertions.*;
import static study.querydsl.entity.QMember.*;
import static study.querydsl.entity.QTeam.*;

@Transactional
@SpringBootTest
public class QuerydslBasicTest {

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

        List<Member> members = em.createQuery("select m from Member m", Member.class)
            .getResultList();

        for (Member member : members) {
            System.out.println("member = " + member);
            System.out.println("-> member.team" + member.getTeam());
        }
    }

    @Test
    void startJPQL() {
        // 1. JPQL 로 member1 찾기
        Member findMember = em.createQuery("select m from Member m where m.username = :username", 
                Member.class)
            .setParameter("username", "memberA")
            .getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("memberA");
    }

    // 2. Querydsl 사용하기
    @Test
    public void startQuerydsl() {
//        QMember m = new QMember("m");
//        QMember m = QMember.member;
//        Member findMember = queryFactory
//            .select(m)
//            .from(m)
//            .where(m.username.eq("memberA"))
//            .fetchOne();

//        Member findMember = queryFactory
//            .select(QMember.member)
//            .from(QMember.member)
//            .where(QMember.member.username.eq("memberA"))
//            .fetchOne();

        // QMember static import 가능
        // 가장 권장하는 방법
        // 같은 테이블을 조인하는 경우에만 따로 선언해서 쓰자.
        Member findMember = queryFactory
            .select(member)
            .from(member)
            .where(member.username.eq("memberA"))
            .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("memberA");

    }

    // 3. 검색조건 Querydsl 사용하기
    // 3-1. eq(), ne(), eq().not()
    // 3-2. isNotnull()
    // 3-3. in(), notInd(), between()
    // 3-4. goe(>=), gt(>), loe(<=), lt(<)
    //  age.goe(30) -> age 가 30 보다 크거나 같은 것들
    // 3-5. like(like 검색), contains(like %aaa% 검색), startsWith(like aaa% 검색)
    @Test
    void search() {
        Member findMember = queryFactory
            .selectFrom(member)
            .where(member.username.eq("memberA").and(member.age.between(10, 30)))
            .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("memberA");
    }

    // and 인 경우 and() 말고 콤마로 붙여도 된다.
    @Test
    void searchAndParam() {
        Member findMember = queryFactory
            .selectFrom(member)
            .where(member.username.eq("memberA"), (member.age.eq(10)))
            .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("memberA");
    }

    // 4. 결과 조회
    // 4-1. fetch() 리스트 조회, 데이터가 없다면 빈 리스트 반환
    // 4-2. fetchOne() 단건 조회, 데이터가 없다면 null, 둘 이상이면 논유니크 예외 발생
    // 4-3. fetchFirst() == limit(1).fetchOne()
    @Test
    void resultFetchTest() {
        List<Member> fetch = queryFactory.selectFrom(member)
            .fetch();

        // 현재 Member 에는 4개의 데이터가 있기 때문에 fetchOne() 사용 불
//        Member findOne = queryFactory.selectFrom(QMember.member)
//            .fetchOne();

        Member fetchFirst = queryFactory.selectFrom(member)
            .fetchFirst();

        // 강의의 fetchResult() 는 앞으로 지원되지 않기 때문에 따로 써줘야한다.
        List<Member> fetch1 = queryFactory.selectFrom(member)
            .where(member.age.eq(10))
            .offset(1)
            .limit(10)
            .fetch();

        // 강의의 fetchCount() 또한 앞으로 지원되지 않기 때문에 따로 사용해줘야한다.
        Long memberCount = queryFactory.select(member.count())
            .from(member)
            .fetchOne();

        assertThat(fetch.get(0).getUsername()).isEqualTo("memberA");
//        assertThat(findOne.getUsername()).isEqualTo("memberA");
        assertThat(fetchFirst.getUsername()).isEqualTo("memberA");
        assertThat(fetch1.get(0).getUsername()).isEqualTo("memberA");
        assertThat(memberCount).isEqualTo(4);
    }

    // 5. 정렬
    // 1. 나이 내림차순
    // 2. 회원 이름 오름차순
    // 2-1. 단, 회원 이름이 없다면 마지막에 출력 (nulls last)
    @Test
    void sort() {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = queryFactory
            .selectFrom(member)
            .where(member.age.eq(100))
            .orderBy(member.age.desc(), member.username.asc().nullsLast())
            .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);

        // 이 순서대로 정렬되야 정상.
        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();
    }

    // 6. 페이징
    // orderBy 를 넣어줘야 페이징이 잘 작동하는지 알 수 있다.
    @Test
    void paging1() {
        List<Member> result = queryFactory
            .selectFrom(member)
            .orderBy(member.username.desc())
            .offset(1)
            .limit(2)
            .fetch();

        assertThat(result.size()).isEqualTo(2);
    }

    // 7. 집합
    @Test
    void aggregation() {

        // 집합을 사용하면 Querydsl 이 제공하는 Tuple 타입으로 반환된다.
        // 여러가지 타입을 받을 때 Tuple 을 쓰는데, 실무에서는 DTO 를 만들어서 그걸 많이 쓴다.
        List<Tuple> result = queryFactory
            .select(
                member.count(),
                member.age.sum(),
                member.age.avg(),
                member.age.max(),
                member.age.min()
            )
            .from(member)
            .fetch();

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);

    }

    // 7-1. Group By
    /**
     * 팀의 이름과 각 팀의 평균 연령을 구해라.
     * from(member) -> Member 테이블과
     * join(member.team, team) -> Member 의 Team ID 와 같은 Team 테이블을 Join 하고,
     * select(team.name, member.age.avg()) -> Team 의 이름과 Member 의 평균 나이를 가져온 뒤,
     * groupBy(team.name) -> Team 이름으로 정렬한다.
     */
    // groupBy() 이후 결과를 제한하려면 having() 을 사용하면 된다.
    @Test
    @Commit
    void group() {
        List<Tuple> result = queryFactory
            .select(team.name, member.age.avg())
            .from(member)
            .join(member.team, team) // team = as team
            .groupBy(team.name) // 팀의 이름으로 그루핑
            .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15);
        assertThat(teamB.get(member.age.avg())).isEqualTo(35);

    }

    // 8. 조인
    /**
     * Team A 에 소속된 모든 회원을 조회한다.
     */
    @Test
    void join() {
        List<Member> result = queryFactory
            .selectFrom(member)
            .join(member.team, team)
            .where(team.name.eq("teamA"))
            .fetch();

        assertThat(result)
            .extracting("username") // Collection 에서 활용가능 username 확인
            .containsExactly("memberA", "memberB"); // 순서를 포함해 정확히 일치하는지 확인
    }

    // 8-1. 쎄타 조인
    /**
     * 회원의 이름이 팀 이름과 같은 회원을 조회
     * 억지성이 있긴 하지만 연관관계가 없는 조인도 가능하다는 것을 보여주기 위한 예제
     * 나열된 모든 테이블을 한번에 가져오고 where 절에서 필터링
     * 쎄타 조인인 경우 외부 조인이 불가능하다. 내부 조인인 inner join 만 가능. on 을 사용하면 가능하긴 하다.
     */
    @Test
    void theta_join() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Member> result = queryFactory
            .select(member)
            .from(member, team) // 쎄타 조인에서는 from 절에 조인을 위한 테이블을 나열한다.
            .where(member.username.eq(team.name))
            .fetch();

        assertThat(result)
            .extracting("username")
            .containsExactly("teamA", "teamB");
    }

    // 8-2. 조인 on 절
    /**
     * 조인 대상을 필터링 하는데 사용
     * 예) 회원과 팀을 조인하면서 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회해라.
     * select m, t from Member m left join m.team t on t.name = 'teamA'
     */
    @Test
    void join_on_filtering() {
        List<Tuple> result = queryFactory
            .select(member, team)
            .from(member)
            .leftJoin(member.team, team) // member 기준 left join 이기 때문에 member 는 일단 다 가져온다.
                .on(team.name.eq("teamA")) // inner join 이면 where 사용과 동일하기 때문에 의미가 없다.
            .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * 연관관계가 없는 엔티티를 외부 조인할 때
     * 회원의 이름이 팀 이름과 같은 대상을 외부 조인해라.
     * 세타 조인은 외부 조인이 불가능하지만 on 절을 사용하면 가능해진다.
     * on 절이 없다면 outer join 이 불가능했던 거기 때문에 inner join 도 물론 가능하다.
     */
    @Test
    void join_on_no_relation() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> result = queryFactory
            .select(member, team)
            .from(member)
            // leftJoin(member.team, team) 이 아닌 from(member) leftJoin(team) 을 적고 on 절로 필터링한다.
            // Join 절에 member.team, team 이 적혀있다면 on 절에 ID 값이 들어가기 때문에 매칭이 되지만,아래처럼 뺴면 일반적인 조인이 아닌
            // member 의 이름과 team 의 이름이 같은 걸로 매칭을 시켜 조인하는 형태가 된다.
            .leftJoin(team).on(member.username.eq(team.name))
            .where(member.username.eq(team.name))
            .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    void fetchJoinNo() {
        // fetch join 시 영속성 컨텍스트를 깔끔하게 비워주지 않으면 정확한 결과를 보기 힘들다.
        em.flush();
        em.clear();

        Member findMember = queryFactory
            .selectFrom(member)
            .where(member.username.eq("memberA"))
            .fetchOne();

        // isLoaded(T) -> 해당 엔티티 내 연관 엔티티가 로딩된 엔티티인지 아닌지 알려준다.
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("패치 조인 미적용").isFalse();
    }

    @Test
    void fetchJoinUse() {
        // fetch join 시 영속성 컨텍스트를 깔끔하게 비워주지 않으면 정확한 결과를 보기 힘들다.
        em.flush();
        em.clear();

        Member findMember = queryFactory
            .selectFrom(member)
            .join(member.team, team).fetchJoin()
            .where(member.username.eq("memberA"))
            .fetchOne();

        // isLoaded(T) -> 해당 엔티티 내 연관 엔티티가 로딩된 엔티티인지 아닌지 알려준다.
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("패치 조인 미적용").isTrue();
    }

    // 9. 서브쿼리 (from 서브쿼리는 지원하지 않는다. 아래 예제는 Where, Select 서브쿼리 예제)
    // 1. 서브쿼리를 join 으로 바꾼다.
    // 2. 애플리케이션에서 쿼리를 2번 분리해서 실행한다.
    // 3. nativeSQL 을 사용한다.
    // -> From 에서 서브쿼리가 필요한 경우 상황에 맞게 1~3 방법 중 택일한다.
    /**
     * 나이가 가장 많은 회원 조회
     * alias 가 같지 않기 위해 Q 타입을 하나 새로 생성한다.
     */
    @Test
    void subQuery() {

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
            .selectFrom(member)
            .where(member.age.eq(
                select(memberSub.age.max())
                    .from(memberSub) // 서브쿼리 결과 40
            ))
            .fetch();

        assertThat(result).extracting("age")
            .containsExactly(40);
    }

    // 9-1. 서브쿼리
    /**
     * 나이가 평균 이상인 회원 조회
     */
    @Test
    void subQueryGoe() {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory.select(member)
            .from(member)
            .where(member.age.goe(
                select(memberSub.age.avg())
                    .from(memberSub)
            ))
            .fetch();

        // 일반 쿼리를 사용하면 회원이 아닌 정수형 리스트가 반환된다.
        List<Integer> fetch = queryFactory.select(member.age.max())
            .from(member)
            .fetch();

        assertThat(result).extracting("age")
            .containsExactly(30, 40);
    }

    // 9-2. 서브쿼리
    /**
     * 나이가 10 이상인 회원 조회
     */
    @Test
    void subQueryIn() {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory.select(member)
            .from(member)
            .where(member.age.in(
                select(memberSub.age)
                    .from(memberSub)
                    .where(memberSub.age.gt(10))
            ))
            .fetch();

        assertThat(result).extracting("age")
            .containsExactly(20, 30, 40);
    }

    // 9-3. Select 서브쿼리
    /**
     * 나이가 평균 이상인 회원 조회
     */
    @Test
    void selectSubQuery() {
        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = queryFactory
            .select(member.username,
                select(memberSub.age.avg())
                    .from(memberSub))
            .from(member)
            .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    // 10. case 문
    // case 문이 꼭 필요한 곳에서만 사용한다. 최대한 이런건 프레젠테이션 레이어, 또는 어플리케이션 로직에서 해결하자.
    @Test
    void basicCase() {
        List<String> result = queryFactory.select(member.age
                .when(10).then(("열 살"))
                .when(20).then("스무 살")
                .otherwise("기타"))
            .from(member)
            .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    // 10-1. 복잡한 case 문
    // CaseBuilder() 사용
    @Test
    void complexCase() {
        List<String> result = queryFactory
            .select(new CaseBuilder()
                .when(member.age.between(0, 20)).then("0~20살")
                .when(member.age.between(21, 30)).then("21~30살")
                .otherwise("기타")
            )
            .from(member)
            .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    // 11. 상수
    @Test
    void constant() {
        List<Tuple> result = queryFactory
            .select(member.username, Expressions.constant("A"))
            .from(member)
            .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    // 12. 문자 더하기
    @Test
    void concat() {
        List<String> result = queryFactory
            // String username, int age 타입을 맞추기 위해 stringValue() 사용
            // 특히 stringValue() 는 ENUM 타입을 처리할 때 자주 사용된다.
            .select(member.username.concat("_").concat(member.age.stringValue()))
            .from(member)
            .where(member.username.eq("memberA"))
            .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

}
