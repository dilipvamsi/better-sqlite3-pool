import "reflect-metadata";
import { DataSource, Entity, PrimaryGeneratedColumn, Column, OneToMany, ManyToOne } from "typeorm";
import { expect } from "chai";
import * as path from "path";
import * as fs from "fs";
import { adapter } from "better-sqlite3-pool";

@Entity()
class User {
    @PrimaryGeneratedColumn()
    id!: number;

    @Column()
    name!: string;

    @OneToMany(() => Post, (post: Post) => post.user)
    posts!: Post[];
}

@Entity()
class Post {
    @PrimaryGeneratedColumn()
    id!: number;

    @Column()
    title!: string;

    @ManyToOne(() => User, (user: User) => user.posts)
    user!: User;
}

describe("TypeScript TypeORM Integration via SQLite3Adapter", function (this: Mocha.Suite) {
    this.timeout(30000);
    const dbPath = path.join(__dirname, "ts-typeorm-test.db");
    let dataSource: DataSource;

    before(async function (this: Mocha.Context) {
        if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);

        dataSource = new DataSource({
            type: "sqlite",
            database: dbPath,
            entities: [User, Post],
            synchronize: true,
            logging: false,
            driver: adapter,
        });

        await dataSource.initialize();
    });

    after(async function (this: Mocha.Context) {
        if (dataSource && dataSource.isInitialized) {
            await dataSource.destroy();
        }
        if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
    });

    it("should initialize and synchronize schema with decorators", async () => {
        expect(dataSource.isInitialized).to.be.true;
    });

    it("should perform basic CRUD operations", async () => {
        const userRepository = dataSource.getRepository(User);

        // Create
        const user = new User();
        user.name = "TypeScript User";
        const savedUser = await userRepository.save(user);
        expect(savedUser.id).to.be.a("number");

        // Read
        const foundUser = await userRepository.findOneBy({ id: savedUser.id });
        expect(foundUser?.name).to.equal("TypeScript User");
    });
});
