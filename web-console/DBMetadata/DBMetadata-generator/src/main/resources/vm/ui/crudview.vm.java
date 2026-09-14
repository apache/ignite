package org.vaadin.crudui.demo.ui.view;

import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.router.Route;

import org.vaadin.crudui.crud.impl.GridCrud;
import org.vaadin.crudui.demo.entity.User;
import org.vaadin.crudui.demo.service.UserService;
import org.vaadin.crudui.demo.ui.MainLayout;

@Route(value = "default", layout = MainLayout.class)
public class DefaultView extends VerticalLayout {

	public DefaultView(UserService userService) {
		GridCrud<User> crud = new GridCrud<>(User.class);
		crud.setOperations(
				userService::findAll,
				userService::save,
				userService::save,
				userService::delete);

		add(crud);
		setSizeFull();
	}

}
