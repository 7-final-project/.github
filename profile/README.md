# ${\textsf{\color{orange} 𝗤}}$ 원격 줄서기 시스템 : Qring
${\textsf{\color{#FF4500}큐링(Qring)}}$은 **Queue(줄)** 와 **ring(고리)** 의 결합으로, 사람들이 기다림 없이 효율적으로 줄을 서고 관리할 수 있도록 돕는 **원격 줄서기 시스템**입니다. 
큐링은 여러분께 **기다림 없는 일상**을 제공합니다.

![7팀_브로셔이미지](https://github.com/user-attachments/assets/70f91b4c-d551-4b2b-88a0-3cce5c8a3d0f)

# ${\textsf{\color{orange} 𝗤}}$ 프로젝트 개요
- 대규모 트래픽 처리 원격 대기열 서비스
- 개발 기간 | 2024.12.26 - 2024.01.26
- 팀 Notion | https://www.notion.so/teamsparta/7-1682dc3ef51480c3bd44d37ac4ad02f9
- 배포 링크 | 

# ${\textsf{\color{orange} 𝗤}}$ 프로젝트 목표
🧍🏻 **Kafka를 활용한 안정적 대기열 처리**

- 대규모 트래픽 환경에서도 Kafka를 이용해 대기열을 안정적으로 관리하며, 시스템의 확장성과 성능을 보장합니다.

🧍‍♂️ **동시성 이슈 해결을 통한 안정적 쿠폰 발급**

- 여러 사용자가 동시에 쿠폰을 발급받더라도 충돌 없이 안정적으로 관리할 수 있도록 동시성 이슈를 효과적으로 처리합니다.

🧍🏻 **실시간 대기열 추적 및 알림 전송**

- 대기열 상태를 실시간으로 추적하며, 사용자의 차례가 다가오면 즉시 알림을 전송하여 편리한 사용자 경험을 제공합니다.

🧍‍♂️ **ELK 스택을 활용한 로그 관리**

- ELK 스택을 통해 로그를 관리하고, 실시간 이벤트 추적을 통해 시스템 안정성을 높입니다.

# ${\textsf{\color{orange} 𝗤}}$ 인프라 설계도
![큐링_인프라_설계도 drawio](https://github.com/user-attachments/assets/2d9ca5b3-3309-4122-8102-e9f8399c9b08)

# ${\textsf{\color{orange} 𝗤}}$ 주요 기능
### 🌐 게이트웨이
- Passport 토큰 발급
    - 유저 인증 시에 Passport 라는 id 토큰을 트랜잭션 내로 전파
    - 유저 정보가 필요한 서비스는 유저 정보 호출 없이 Passport 정보를 통하여 유저에 대한 정보를 사용할 수 있음
    - **짧은 TTL 로 캐시 및 redis pub/sub 을 통해 유저 정보 갱신 최적화**
    - 참고자료 : https://toss.tech/article/slash23-server
- Global-Transaction-Id 발급 및 ELK 스택 환경 로그 관리
    - 유저 요청 시 최초로 **Global-Transaction-Id**를 발급하여 모든 트랙잭션에 전파
    - 이후 모든 유저 요청은 **Global-Transaction-Id**를 포함하여 발송되며, 이를 통해 트랜잭션의 흐름을 추적할 수 있음.
    - ELK 스택을 활용하여 트랜잭션 로그를 실시간으로 수집, 분석, 시각화.

### 🧍🏻 대기열
- Redis 기반 실시간 대기열 관리
    - **Redis SortedSet**을 활용하여 실시간 대기 순번 관리 및 조회 가능
    - Redis **KeySpaceNotification**을 사용하여 대기열 상태 변화를 실시간으로 감지하고 특정 이벤트를 트리거.
    - 각 서버에 특정 가게를 할당하여 대기열 부하를 분산하고, 실시간 대기 상태를 중앙에서 관리.

### 🎟️ 쿠폰
- 락을 활용한 동시성 제어
    - 쿠폰 발급 시 비관적락 적용
    - 동시성 확보를 통해 쿠폰 재고 수량 초과 발급 방지
    - 트랜잭션 분리를 통해 락 범위 최소화
      `@Transactional(propagation = Propagation.*REQUIRED*)`

### 🍴식당
- 운영 시간 기반 자동 상태 업데이트
    - 사용자가 식당을 생성할 때 운영 시간을 함께 설정 가능.
    - **TaskScheduler**를 활용하여 설정된 운영 요일 및 시간에 따라 식당의 상태를 `OPEN` 또는 `CLOSED`로 자동 변경.
    - 운영 상태 변경 스케줄링은 `OperationStatusScheduler`를 통해 동적으로 관리.
- 실시간 평점 계산 및 통계 관리
    - Kafka 메시지를 소비하여 리뷰 이벤트(`CREATE`, `UPDATE`, `DELETE`)에 따라 `reviewCount`와 `totalRating`을 업데이트.
    - 조회 시점에 `reviewCount`와 `totalRating`을 기반으로 평균 평점(`averageRating`)을 실시간으로 계산하여 반환.

### 📝 리뷰
- Kafka 기반 이벤트 발행
    - 리뷰 생성, 수정, 삭제 시 이벤트를 Kafka 메시지로 발행하여 식당 서비스가 이를 소비하고 통계를 관리.
    - 메시지 구조를 간소화하여 `restaurantId`, `rating`, `eventType(CREATE, UPDATE, DELETE)`만 포함.
- 예약 정보와 식당 정보 검증
    - Feign Client를 활용하여 리뷰 작성 전 예약 정보와 식당 정보를 검증.
    - 예약 상태와 로그인된 사용자 정보의 일치 여부를 확인하여 데이터의 신뢰성을 확보.

# ${\textsf{\color{orange} 𝗤}}$ 적용 기술 
### 프레임워크 / 라이브러리
![JDK 17](https://img.shields.io/badge/JDK-17-007396?style=flat-square&logo=java&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4.1-6DB33F?style=flat-square&logo=springboot&logoColor=white)
![Eureka](https://img.shields.io/badge/Eureka-6DB33F?style=flat-square&logo=spring&logoColor=white)
![OpenFeign](https://img.shields.io/badge/OpenFeign-6DB33F?style=flat-square&logo=spring&logoColor=white)
![Spring Cloud Gateway](https://img.shields.io/badge/Spring%20Cloud%20Gateway-6DB33F?style=flat-square&logo=spring&logoColor=white)
![Redisson](https://img.shields.io/badge/Redisson-DC382D?style=flat-square&logo=redis&logoColor=white)
![QueryDSL](https://img.shields.io/badge/QueryDSL-5.0.0-6DB33F?style=flat-square&logo=spring&logoColor=white)
![Spring Data JPA](https://img.shields.io/badge/Spring%20Data%20JPA-6DB33F?style=flat-square&logo=spring&logoColor=white)

### 메시징
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)

### 데이터베이스
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16.4-336791?style=flat-square&logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=flat-square&logo=redis&logoColor=white)

### 성능 테스트
![Apache JMeter](https://img.shields.io/badge/Apache%20JMeter-D22128?style=flat-square&logo=apache&logoColor=white)

### 외부 연동
![Slack](https://img.shields.io/badge/Slack-4A154B?style=flat-square&logo=slack&logoColor=white)

### 인프라
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker%20Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=flat-square&logo=githubactions&logoColor=white)
![Jenkins](https://img.shields.io/badge/Jenkins-D24939?style=flat-square&logo=jenkins&logoColor=white)
![AWS EC2](https://img.shields.io/badge/AWS%20EC2-FF9900?style=flat-square&logo=amazonaws&logoColor=white)
![Ubuntu](https://img.shields.io/badge/Ubuntu-E95420?style=flat-square&logo=ubuntu&logoColor=white)

### 협업 툴
![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat-square&logo=github&logoColor=white)
![Slack](https://img.shields.io/badge/Slack-4A154B?style=flat-square&logo=slack&logoColor=white)
![Notion](https://img.shields.io/badge/Notion-000000?style=flat-square&logo=notion&logoColor=white)

# ${\textsf{\color{orange} 𝗤}}$ 기술적 의사결정


# ${\textsf{\color{orange} 𝗤}}$ 트러블 슈팅


# 📊 서비스 상태 모니터링 대시보드
| 서비스 이름         | 상태                                                                                  |
|---------------------|---------------------------------------------------------------------------------------|
| 🚀 **Server**       | ![example workflow](https://github.com/7-final-project/server/actions/workflows/gradle.yml/badge.svg) |
| 🔑 **Auth**         | ![example workflow](https://github.com/7-final-project/auth/actions/workflows/gradle.yml/badge.svg)   |
| 🌐 **Gateway**      | ![example workflow](https://github.com/7-final-project/gateway/actions/workflows/gradle.yml/badge.svg) |
| 🎟️ **Coupon**      | ![example workflow](https://github.com/7-final-project/coupon/actions/workflows/gradle.yml/badge.svg) |
| 📅 **Reservation**  | ![example workflow](https://github.com/7-final-project/reservation/actions/workflows/gradle.yml/badge.svg) |
| ⏳ **Queue**        | ![example workflow](https://github.com/7-final-project/queue/actions/workflows/gradle.yml/badge.svg)  |
| 🍽️ **Restaurant**  | ![example workflow](https://github.com/7-final-project/restaurant/actions/workflows/gradle.yml/badge.svg) |
| ⭐ **Review**        | ![example workflow](https://github.com/7-final-project/review/actions/workflows/gradle.yml/badge.svg) |
| 💬 **Message**      | ![example workflow](https://github.com/7-final-project/message/actions/workflows/gradle.yml/badge.svg) |
