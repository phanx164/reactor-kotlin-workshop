import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    val kotlinVersion = "1.5.0"
    id("org.jetbrains.kotlin.jvm") version kotlinVersion
    id("io.spring.dependency-management") version "1.0.8.RELEASE"
}

repositories {
    mavenCentral()
    maven {
        setUrl("https://repo.spring.io/milestone")
    }
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        jvmTarget = "1.8"
    }
}

dependencyManagement {
    imports {
        mavenBom("io.projectreactor:reactor-bom:2020.0.2")
    }
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib")
    implementation("io.projectreactor:reactor-core")
    implementation("io.projectreactor:reactor-test")
    implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")
    implementation("io.reactivex.rxjava2:rxjava:2.0.5")
    testImplementation("junit:junit:4.12")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.8.2")
}
